WITH player_actions AS (
    SELECT
        pa.*
    FROM {{ ref('int_player_actions') }} pa
    JOIN {{ ref('stg_hands') }} h ON pa.hand_id = h.hand_id
),

showdown_results AS (
    SELECT
        sr.player_id,
        sr.hand_id,
        sr.went_to_showdown,
        sr.won_at_showdown,
        COALESCE(sr.net_profit, 0) as net_profit,  -- NULLを0に変換
        COALESCE(sr.total_invested, 0) as total_invested,  -- NULLを0に変換
        -- ホールカードの状態判定を追加
        CASE
            WHEN ARRAY_SIZE(sr.hole_cards) = 0 THEN 0  -- フォールドしたプレイヤー
            ELSE 1  -- アクティブなプレイヤー
        END as has_hole_cards
    FROM {{ ref('int_player_hands') }} sr
    INNER JOIN {{ ref('stg_hands') }} h ON sr.hand_id = h.hand_id
    WHERE h.game_type = 'TOURNAMENT'
),

player_stats AS (
    SELECT
        pa.player_id,
        -- 基本統計
        COUNT(DISTINCT pa.hand_id) as total_hands,
        -- VPIP
        COUNT(DISTINCT CASE WHEN pa.is_vpip = 1 THEN pa.hand_id END) as vpip_hands,
        -- PFR
        COUNT(DISTINCT CASE WHEN pa.is_pfr = 1 THEN pa.hand_id END) as pfr_hands,
        -- 3BET
        COUNT(DISTINCT CASE WHEN pa.is_3bet = 1 THEN pa.hand_id END) as threeBet_hands,
        -- FLOP CB
        COUNT(DISTINCT CASE WHEN pa.cbet_flop = 1 THEN pa.hand_id END) as flopCB_hands,
        -- アグレッション（アクティブなプレイヤーのみ）
        SUM(CASE WHEN sr.has_hole_cards = 1 THEN pa.aggressive_actions ELSE 0 END) as aggressive_actions,
        SUM(CASE WHEN sr.has_hole_cards = 1 THEN pa.passive_actions ELSE 0 END) as passive_actions,
        SUM(CASE WHEN sr.has_hole_cards = 1 THEN pa.aggressive_actions + pa.passive_actions ELSE 0 END) as total_noncheck_actions,
        -- ショーダウン統計（アクティブなプレイヤーのみ）
        COUNT(DISTINCT CASE WHEN sr.has_hole_cards = 1 AND sr.went_to_showdown = 1 THEN sr.hand_id END) as showdown_hands,
        COUNT(DISTINCT CASE WHEN sr.has_hole_cards = 1 AND sr.won_at_showdown = 1 THEN sr.hand_id END) as won_at_showdown_hands,
        -- フロップ到達（アクティブなプレイヤーのみ）
        COUNT(DISTINCT CASE WHEN sr.has_hole_cards = 1 AND pa.phase_name = 'FLOP' THEN pa.hand_id END) as saw_flop_hands,
        COUNT(DISTINCT CASE WHEN sr.has_hole_cards = 1 AND pa.phase_name = 'FLOP' AND sr.won_at_showdown = 1 THEN pa.hand_id END) as won_after_flop_hands,
        -- ターン到達（アクティブなプレイヤーのみ）
        COUNT(DISTINCT CASE WHEN sr.has_hole_cards = 1 AND pa.phase_name = 'TURN' THEN pa.hand_id END) as saw_turn_hands,
        COUNT(DISTINCT CASE WHEN sr.has_hole_cards = 1 AND pa.phase_name = 'TURN' AND sr.won_at_showdown = 1 THEN pa.hand_id END) as won_after_turn_hands,
        -- リバー到達（アクティブなプレイヤーのみ）
        COUNT(DISTINCT CASE WHEN sr.has_hole_cards = 1 AND pa.phase_name = 'RIVER' THEN pa.hand_id END) as saw_river_hands,
        COUNT(DISTINCT CASE WHEN sr.has_hole_cards = 1 AND pa.phase_name = 'RIVER' AND sr.won_at_showdown = 1 THEN pa.hand_id END) as won_after_river_hands,
        -- 収益性指標
        SUM(sr.net_profit) as total_profit,
        SUM(sr.total_invested) as total_invested,
        AVG(sr.net_profit) as avg_profit_per_hand
    FROM player_actions pa
    LEFT JOIN showdown_results sr ON pa.player_id = sr.player_id AND pa.hand_id = sr.hand_id
    GROUP BY pa.player_id
)

SELECT
    player_id,
    total_hands,
    -- VPIP
    vpip_hands,
    COALESCE(LEAST(1, ROUND(vpip_hands::FLOAT / NULLIF(total_hands, 0), 4)), 0) as vpip_ratio,
    -- PFR
    pfr_hands,
    COALESCE(LEAST(1, ROUND(pfr_hands::FLOAT / NULLIF(total_hands, 0), 4)), 0) as pfr_ratio,
    -- 3BET
    threeBet_hands,
    COALESCE(LEAST(1, ROUND(threeBet_hands::FLOAT / NULLIF(total_hands, 0), 4)), 0) as threeBet_ratio,
    -- FLOP CB
    flopCB_hands,
    COALESCE(LEAST(1, ROUND(flopCB_hands::FLOAT / NULLIF(saw_flop_hands, 0), 4)), 0) as flopCB_ratio,
    -- アグレッション
    COALESCE(ROUND(aggressive_actions::FLOAT / NULLIF(passive_actions, 0), 4), 0) as aggression_factor,
    COALESCE(LEAST(1, ROUND(aggressive_actions::FLOAT / NULLIF(total_noncheck_actions, 0), 4)), 0) as aggression_frequency,
    -- ショーダウン統計
    COALESCE(LEAST(1, ROUND(showdown_hands::FLOAT / NULLIF(saw_flop_hands, 0), 4)), 0) as went_to_showdown_ratio,
    COALESCE(LEAST(1, ROUND(won_after_flop_hands::FLOAT / NULLIF(saw_flop_hands, 0), 4)), 0) as won_when_saw_flop_ratio,
    COALESCE(LEAST(1, ROUND(won_at_showdown_hands::FLOAT / NULLIF(showdown_hands, 0), 4)), 0) as won_at_showdown_ratio,
    -- ストリート到達率
    COALESCE(LEAST(1, ROUND(saw_flop_hands::FLOAT / NULLIF(total_hands, 0), 4)), 0) as flop_seen_ratio,
    COALESCE(LEAST(1, ROUND(saw_turn_hands::FLOAT / NULLIF(saw_flop_hands, 0), 4)), 0) as turn_seen_ratio,
    COALESCE(LEAST(1, ROUND(saw_river_hands::FLOAT / NULLIF(saw_turn_hands, 0), 4)), 0) as river_seen_ratio,
    -- ストリート別勝率
    COALESCE(LEAST(1, ROUND(won_after_flop_hands::FLOAT / NULLIF(saw_flop_hands, 0), 4)), 0) as won_after_flop_ratio,
    COALESCE(LEAST(1, ROUND(won_after_turn_hands::FLOAT / NULLIF(saw_turn_hands, 0), 4)), 0) as won_after_turn_ratio,
    COALESCE(LEAST(1, ROUND(won_after_river_hands::FLOAT / NULLIF(saw_river_hands, 0), 4)), 0) as won_after_river_ratio,
    -- 収益性指標
    COALESCE(total_profit, 0) as total_profit,
    COALESCE(total_invested, 0) as total_invested,
    COALESCE(avg_profit_per_hand, 0) as avg_profit_per_hand,
    CASE
        WHEN COALESCE(total_invested, 0) = 0 THEN 0
        ELSE ROUND(COALESCE(total_profit, 0)::FLOAT / total_invested, 4)
    END as roi
FROM player_stats
WHERE total_hands >= 10  -- 最低10ハンド以上プレイしたプレイヤーのみを対象
