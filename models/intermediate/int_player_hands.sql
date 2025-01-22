WITH player_actions_base AS (
    SELECT
        hand_id,
        player_id,
        phase,
        action_name,
        action_order,
        bet_chip,
        player_chip,
        LAG(action_name) OVER (PARTITION BY hand_id ORDER BY action_order) as prev_action_name,
        LAG(player_id) OVER (PARTITION BY hand_id ORDER BY action_order) as prev_player_id
    FROM {{ ref('stg_player_actions') }}
),

player_actions AS (
    SELECT
        hand_id,
        player_id,
        -- プリフロップのアクション指標
        MAX(CASE WHEN phase = 0 AND action_name IN ('BET', 'RAISE', 'CALL') THEN 1 ELSE 0 END) AS is_vpip,
        MAX(CASE WHEN phase = 0 AND action_name IN ('BET', 'RAISE') THEN 1 ELSE 0 END) AS is_pfr,
        MAX(CASE WHEN phase = 0 AND action_name = 'RAISE' AND prev_action_name = 'RAISE' THEN 1 ELSE 0 END) AS is_3bet,
        -- フロップのアクション指標
        MAX(CASE
            WHEN phase = 1
            AND action_name IN ('BET', 'RAISE')
            AND EXISTS (
                SELECT 1
                FROM {{ ref('stg_player_actions') }} pa2
                WHERE pa2.hand_id = pa1.hand_id
                AND pa2.player_id = pa1.player_id
                AND pa2.phase = 0
                AND pa2.action_name IN ('BET', 'RAISE')
            )
            THEN 1 ELSE 0 END
        ) AS cbet_flop,
        -- CBetに対するフォールド
        MAX(CASE
            WHEN action_name = 'FOLD'
            AND prev_action_name IN ('BET', 'RAISE')
            AND EXISTS (
                SELECT 1
                FROM {{ ref('stg_player_actions') }} pa2
                WHERE pa2.hand_id = pa1.hand_id
                AND pa2.player_id = prev_player_id
                AND pa2.phase = 0
                AND pa2.action_name IN ('BET', 'RAISE')
            )
            THEN 1 ELSE 0 END
        ) AS fold_to_cbet,
        -- アグレッション指標
        SUM(CASE WHEN action_name IN ('BET', 'RAISE', 'ALL_IN') THEN 1 ELSE 0 END) AS aggressive_actions,
        SUM(CASE WHEN action_name = 'CALL' THEN 1 ELSE 0 END) AS passive_actions,
        -- スタック情報
        COALESCE(MIN(player_chip), 0) AS starting_stack,
        COALESCE(MAX(player_chip), 0) AS final_stack,
        COALESCE(SUM(bet_chip), 0) AS total_invested,
        -- プレッシャー指標の平均
        COALESCE(AVG(bet_chip::FLOAT), 0) AS pressure_index
    FROM player_actions_base pa1
    GROUP BY hand_id, player_id
),

hand_results AS (
    SELECT
        hand_id,
        player_id,
        hole_cards,
        hole_cards_str,
        -- ハンド文字列（例：AKs, AA, 72o）
        CASE
            WHEN (hole_cards[0] BETWEEN 0 AND 51) AND (hole_cards[1] BETWEEN 0 AND 51) THEN
                CASE
                    -- ポケットペア判定
                    WHEN FLOOR(hole_cards[0]::NUMBER / 4) = FLOOR(hole_cards[1]::NUMBER / 4) THEN
                        CASE
                            WHEN FLOOR(hole_cards[0]::NUMBER / 4) = 12 THEN 'AA'
                            WHEN FLOOR(hole_cards[0]::NUMBER / 4) = 11 THEN 'KK'
                            WHEN FLOOR(hole_cards[0]::NUMBER / 4) = 10 THEN 'QQ'
                            WHEN FLOOR(hole_cards[0]::NUMBER / 4) = 9  THEN 'JJ'
                            WHEN FLOOR(hole_cards[0]::NUMBER / 4) = 8  THEN 'TT'
                            ELSE LPAD(TO_CHAR(FLOOR(hole_cards[0]::NUMBER / 4) + 2), 2, TO_CHAR(FLOOR(hole_cards[0]::NUMBER / 4) + 2))
                        END
                    ELSE
                        -- 通常の2枚組
                        CASE
                            WHEN GREATEST(FLOOR(hole_cards[0]::NUMBER / 4), FLOOR(hole_cards[1]::NUMBER / 4)) = 12 THEN 'A'
                            WHEN GREATEST(FLOOR(hole_cards[0]::NUMBER / 4), FLOOR(hole_cards[1]::NUMBER / 4)) = 11 THEN 'K'
                            WHEN GREATEST(FLOOR(hole_cards[0]::NUMBER / 4), FLOOR(hole_cards[1]::NUMBER / 4)) = 10 THEN 'Q'
                            WHEN GREATEST(FLOOR(hole_cards[0]::NUMBER / 4), FLOOR(hole_cards[1]::NUMBER / 4)) = 9  THEN 'J'
                            WHEN GREATEST(FLOOR(hole_cards[0]::NUMBER / 4), FLOOR(hole_cards[1]::NUMBER / 4)) = 8  THEN 'T'
                            ELSE TO_CHAR(GREATEST(FLOOR(hole_cards[0]::NUMBER / 4), FLOOR(hole_cards[1]::NUMBER / 4)) + 2)
                        END
                        ||
                        CASE
                            WHEN LEAST(FLOOR(hole_cards[0]::NUMBER / 4), FLOOR(hole_cards[1]::NUMBER / 4)) = 12 THEN 'A'
                            WHEN LEAST(FLOOR(hole_cards[0]::NUMBER / 4), FLOOR(hole_cards[1]::NUMBER / 4)) = 11 THEN 'K'
                            WHEN LEAST(FLOOR(hole_cards[0]::NUMBER / 4), FLOOR(hole_cards[1]::NUMBER / 4)) = 10 THEN 'Q'
                            WHEN LEAST(FLOOR(hole_cards[0]::NUMBER / 4), FLOOR(hole_cards[1]::NUMBER / 4)) = 9  THEN 'J'
                            WHEN LEAST(FLOOR(hole_cards[0]::NUMBER / 4), FLOOR(hole_cards[1]::NUMBER / 4)) = 8  THEN 'T'
                            ELSE TO_CHAR(LEAST(FLOOR(hole_cards[0]::NUMBER / 4), FLOOR(hole_cards[1]::NUMBER / 4)) + 2)
                        END
                        ||
                        CASE WHEN hole_cards[0]::NUMBER % 4 = hole_cards[1]::NUMBER % 4 THEN 's' ELSE 'o' END
                END
            ELSE NULL
        END AS hand_str,
        -- ハンドタイプの判定
        CASE
            WHEN (hole_cards[0] BETWEEN 0 AND 51) AND (hole_cards[1] BETWEEN 0 AND 51) THEN
                CASE
                    WHEN FLOOR(hole_cards[0]::NUMBER / 4) = FLOOR(hole_cards[1]::NUMBER / 4) THEN 'POCKET_PAIR'
                    WHEN hole_cards[0]::NUMBER % 4 = hole_cards[1]::NUMBER % 4
                        AND ABS(FLOOR(hole_cards[0]::NUMBER / 4) - FLOOR(hole_cards[1]::NUMBER / 4)) = 1 THEN 'SUITED_CONNECTOR'
                    WHEN hole_cards[0]::NUMBER % 4 = hole_cards[1]::NUMBER % 4 THEN 'SUITED'
                    WHEN ABS(FLOOR(hole_cards[0]::NUMBER / 4) - FLOOR(hole_cards[1]::NUMBER / 4)) = 1 THEN 'CONNECTOR'
                    ELSE 'OFFSUIT'
                END
            ELSE NULL
        END AS hand_type,
        -- ハンドカテゴリーの判定
        CASE
            WHEN (hole_cards[0] BETWEEN 0 AND 51) AND (hole_cards[1] BETWEEN 0 AND 51) THEN
                CASE
                    WHEN FLOOR(hole_cards[0]::NUMBER / 4) = FLOOR(hole_cards[1]::NUMBER / 4) AND FLOOR(hole_cards[0]::NUMBER / 4) >= 9 THEN 'PREMIUM_PAIR'
                    WHEN FLOOR(hole_cards[0]::NUMBER / 4) = FLOOR(hole_cards[1]::NUMBER / 4) AND FLOOR(hole_cards[0]::NUMBER / 4) >= 6 THEN 'MEDIUM_PAIR'
                    WHEN FLOOR(hole_cards[0]::NUMBER / 4) = FLOOR(hole_cards[1]::NUMBER / 4) THEN 'SMALL_PAIR'
                    WHEN FLOOR(hole_cards[0]::NUMBER / 4) >= 9 AND FLOOR(hole_cards[1]::NUMBER / 4) >= 9 THEN 'PREMIUM_CARDS'
                    WHEN FLOOR(hole_cards[0]::NUMBER / 4) >= 6 AND FLOOR(hole_cards[1]::NUMBER / 4) >= 6 THEN 'MEDIUM_CARDS'
                    ELSE 'SMALL_CARDS'
                END
            ELSE NULL
        END AS hand_category,
        hand_ranking,
        CASE WHEN reward_chip IS NOT NULL THEN 1 ELSE 0 END AS went_to_showdown,
        CASE WHEN reward_chip > 0 THEN 1 ELSE 0 END AS won_at_showdown,
        COALESCE(reward_chip, 0) AS net_profit
    FROM {{ ref('stg_hand_results') }}
),

position_info AS (
    SELECT
        h.hand_id,
        pa.player_id,
        pa.seat_index,
        CASE
            WHEN pa.seat_index = h.button_seat THEN 'BTN'
            WHEN pa.seat_index = h.sb_seat THEN 'SB'
            WHEN pa.seat_index = h.bb_seat THEN 'BB'
            WHEN (pa.seat_index + 6 - h.button_seat) % 6 = 3 THEN 'UTG'
            WHEN (pa.seat_index + 6 - h.button_seat) % 6 = 4 THEN 'HJ'
            WHEN (pa.seat_index + 6 - h.button_seat) % 6 = 5 THEN 'CO'
        END AS position,
        h.bb
    FROM {{ ref('stg_hands') }} h
    CROSS JOIN (
        SELECT DISTINCT hand_id, player_id, seat_index
        FROM {{ ref('stg_player_actions') }}
    ) pa
    WHERE h.hand_id = pa.hand_id
)

SELECT
    -- 基本情報
    hr.hand_id,
    hr.player_id,
    pi.seat_index,
    pi.position,
    -- ホールカード情報
    hr.hole_cards,
    hr.hole_cards_str,
    hr.hand_str,
    hr.hand_type,
    hr.hand_category,
    hr.hand_ranking,
    -- スタック情報
    COALESCE(pa.starting_stack, 0) AS starting_stack,
    COALESCE(pa.final_stack, 0) AS final_stack,
    COALESCE(pa.total_invested, 0) AS total_invested,
    -- Mレシオ（スタック / BB）
    COALESCE(
        CASE
            WHEN COALESCE(pi.bb, 0) > 0 THEN pa.starting_stack::FLOAT / pi.bb
            ELSE 0
        END,
        0
    ) AS m_ratio,
    -- 実効スタック（自分と相手の小さい方のスタック）
    COALESCE(pa.starting_stack, 0) AS effective_stack,
    -- アクション指標
    pa.is_vpip,
    pa.is_pfr,
    pa.is_3bet,
    pa.cbet_flop,
    pa.fold_to_cbet,
    -- 結果情報
    hr.went_to_showdown,
    hr.won_at_showdown,
    hr.net_profit,
    -- アグレッション指標
    CASE
        WHEN COALESCE(pa.passive_actions, 0) > 0 THEN pa.aggressive_actions::FLOAT / pa.passive_actions
        ELSE COALESCE(pa.aggressive_actions::FLOAT, 0)
    END AS aggression_factor,
    pa.pressure_index
FROM hand_results hr
JOIN position_info pi ON hr.hand_id = pi.hand_id AND hr.player_id = pi.player_id
JOIN player_actions pa ON hr.hand_id = pa.hand_id AND hr.player_id = pa.player_id
