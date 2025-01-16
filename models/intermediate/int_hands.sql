WITH hand_results AS (
    SELECT
        hand_id,
        player_id,
        hole_cards_str,
        -- ハンドタイプの判定
        CASE
            WHEN hole_cards_str[0] IS NOT NULL AND hole_cards_str[1] IS NOT NULL THEN
                CASE
                    WHEN FLOOR(hole_cards_str[0] / 13) = FLOOR(hole_cards_str[1] / 13) THEN 'POCKET_PAIR'
                    WHEN MOD(hole_cards_str[0], 13) = MOD(hole_cards_str[1], 13) AND ABS(FLOOR(hole_cards_str[0] / 13) - FLOOR(hole_cards_str[1] / 13)) = 1 THEN 'SUITED_CONNECTOR'
                    WHEN MOD(hole_cards_str[0], 13) = MOD(hole_cards_str[1], 13) THEN 'SUITED'
                    WHEN ABS(FLOOR(hole_cards_str[0] / 13) - FLOOR(hole_cards_str[1] / 13)) = 1 THEN 'CONNECTOR'
                    ELSE 'OFFSUIT'
                END
            ELSE NULL
        END AS hand_type,
        -- ハンドカテゴリーの判定
        CASE
            WHEN hole_cards_str[0] IS NOT NULL AND hole_cards_str[1] IS NOT NULL THEN
                CASE
                    WHEN FLOOR(hole_cards_str[0] / 13) = FLOOR(hole_cards_str[1] / 13) AND FLOOR(hole_cards_str[0] / 13) >= 10 THEN 'PREMIUM_PAIR'
                    WHEN FLOOR(hole_cards_str[0] / 13) = FLOOR(hole_cards_str[1] / 13) AND FLOOR(hole_cards_str[0] / 13) >= 7 THEN 'MEDIUM_PAIR'
                    WHEN FLOOR(hole_cards_str[0] / 13) = FLOOR(hole_cards_str[1] / 13) THEN 'SMALL_PAIR'
                    WHEN FLOOR(hole_cards_str[0] / 13) >= 10 AND FLOOR(hole_cards_str[1] / 13) >= 10 THEN 'PREMIUM_CARDS'
                    WHEN FLOOR(hole_cards_str[0] / 13) >= 7 AND FLOOR(hole_cards_str[1] / 13) >= 7 THEN 'MEDIUM_CARDS'
                    ELSE 'SMALL_CARDS'
                END
            ELSE NULL
        END AS hand_category,
        hand_ranking AS hand_strength,
        CASE WHEN reward_chip > 0 THEN 1 ELSE 0 END AS won_at_showdown,
        CASE WHEN reward_chip IS NOT NULL THEN 1 ELSE 0 END AS went_to_showdown,
        COALESCE(reward_chip, 0) AS net_profit
    FROM {{ ref('stg_hand_results') }}
),

showdown_info AS (
    SELECT
        hand_id,
        COUNT(DISTINCT player_id) AS players_at_showdown,
        SUM(CASE WHEN won_at_showdown = 1 THEN 1 ELSE 0 END) AS winners_at_showdown,
        SUM(CASE WHEN net_profit > 0 THEN 1 ELSE 0 END) AS winners_count,
        SUM(net_profit) AS total_profit,
        MAX(hand_strength) AS winning_hand_strength
    FROM hand_results
    GROUP BY hand_id
),

pot_info AS (
    SELECT
        pa.hand_id,
        -- アクティブプレイヤー数の修正：実際にアクションに参加したプレイヤー数
        GREATEST(
            -- 通常のアクティブプレイヤー計算
            COUNT(DISTINCT
                CASE
                    WHEN pa.action_name = 'FOLD' THEN NULL  -- FOLDしたプレイヤーは除外
                    WHEN pa.action_name IN ('BET', 'RAISE', 'CALL', 'ALL_IN') THEN pa.player_id  -- 積極的なアクション
                    WHEN pa.action_name = 'CHECK' AND (
                        pa.phase > 0  -- プリフロップ以外でのCHECK
                        OR (pa.phase = 0 AND h.bb_seat = pa.seat_index)  -- BBポジションでのCHECK
                    ) THEN pa.player_id
                    ELSE NULL
                END
            ),
            -- オールインケースの最小プレイヤー数（2）を保証
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM {{ ref('stg_player_actions') }} pa_all_in
                    WHERE pa_all_in.hand_id = pa.hand_id
                    AND pa_all_in.action_name = 'ALL_IN'
                ) THEN 2
                ELSE 0
            END,
            -- コールケースの最小プレイヤー数（2）を保証
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM {{ ref('stg_player_actions') }} pa_call
                    WHERE pa_call.hand_id = pa.hand_id
                    AND pa_call.action_name = 'CALL'
                ) THEN 2
                ELSE 0
            END,
            -- ポット獲得者が存在する場合は最低1人を保証
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM {{ ref('stg_hand_results') }} hr
                    WHERE hr.hand_id = pa.hand_id
                    AND hr.reward_chip > 0
                ) THEN 1
                ELSE 0
            END,
            -- ショーダウンプレイヤー数を保証
            COALESCE(
                (
                    SELECT COUNT(DISTINCT player_id)
                    FROM {{ ref('stg_hand_results') }} hr
                    WHERE hr.hand_id = pa.hand_id
                    AND hr.reward_chip IS NOT NULL
                ),
                0
            )
        ) AS active_players,
        COUNT(DISTINCT CASE WHEN pa.phase = 0 AND pa.action_name IN ('BET', 'RAISE', 'CALL') THEN pa.player_id END) AS vpip_players,
        COUNT(DISTINCT CASE WHEN pa.phase = 0 AND pa.action_name IN ('BET', 'RAISE') THEN pa.player_id END) AS pfr_players
    FROM {{ ref('stg_player_actions') }} pa
    LEFT JOIN {{ ref('stg_hands') }} h
        ON pa.hand_id = h.hand_id
    GROUP BY pa.hand_id
),

final_pot_info AS (
    SELECT
        hand_id,
        SUM(reward_chip) AS final_pot
    FROM {{ ref('stg_hand_results') }}
    GROUP BY hand_id
),

hand_phases AS (
    SELECT
        hand_id,
        MAX(phase) AS last_phase,
        COUNT(DISTINCT phase) AS total_phases,
        COUNT(DISTINCT CASE WHEN action_name IN ('BET', 'RAISE') THEN phase END) AS betting_phases
    FROM {{ ref('stg_player_actions') }}
    GROUP BY hand_id
),

hand_info AS (
    SELECT
        h.hand_id,
        h.start_timestamp,
        h.game_type,
        h.community_cards,
        h.community_cards_str,
        h.button_seat,
        h.sb_seat,
        h.bb_seat,
        h.bb,
    FROM {{ ref('stg_hands') }} h
)

SELECT
    -- 基本情報
    hi.hand_id,
    hi.start_timestamp,
    hi.game_type,
    hi.button_seat,
    hi.sb_seat,
    hi.bb_seat,
    hi.bb,
    -- コミュニティカード情報
    hi.community_cards,
    hi.community_cards_str,
    -- ポット情報
    fpi.final_pot,
    -- プレイヤー情報
    pi.active_players,
    pi.vpip_players,
    pi.pfr_players,
    si.players_at_showdown,
    si.winners_at_showdown,
    si.winners_count,
    -- フェーズ情報
    hp.last_phase,
    hp.total_phases,
    hp.betting_phases,
    -- 結果情報
    si.total_profit,
    si.winning_hand_strength
FROM hand_info hi
LEFT JOIN pot_info pi ON hi.hand_id = pi.hand_id
LEFT JOIN showdown_info si ON hi.hand_id = si.hand_id
LEFT JOIN hand_phases hp ON hi.hand_id = hp.hand_id
LEFT JOIN final_pot_info fpi ON hi.hand_id = fpi.hand_id
