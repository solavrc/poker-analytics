WITH source AS (
    SELECT *
    FROM {{ ref('stg_player_actions') }}
),

action_metrics AS (
    SELECT
        hand_id,
        player_id,
        phase,
        CASE phase
          WHEN 0 THEN 'PREFLOP'
          WHEN 1 THEN 'FLOP'
          WHEN 2 THEN 'TURN'
          WHEN 3 THEN 'RIVER'
        END as phase_name,
        event_timestamp,
        action_order,
        action_name,
        bet_chip,
        pot,
        player_chip,
        bb,
        -- スタックとポットの比率
        CASE
            WHEN pot = 0 THEN NULL
            ELSE player_chip::FLOAT / pot
        END AS stack_to_pot_ratio,
        -- プレッシャー指標
        CASE
            WHEN bb = 0 THEN NULL
            ELSE bet_chip::FLOAT / bb
        END AS pressure_index,
        -- アクション種別ごとのカウント
        CASE WHEN action_name IN ('BET', 'RAISE', 'ALL_IN') THEN 1 ELSE 0 END AS aggressive_action,
        CASE WHEN action_name = 'CALL' THEN 1 ELSE 0 END AS passive_action,
        CASE WHEN action_name = 'CHECK' THEN 1 ELSE 0 END AS check_action,
        CASE WHEN action_name = 'FOLD' THEN 1 ELSE 0 END AS fold_action
    FROM source
),

action_diversity AS (
    SELECT
        hand_id,
        player_id,
        phase,
        phase_name,
        event_timestamp,
        action_order,
        action_name,
        bet_chip,
        pot,
        player_chip,
        bb,
        stack_to_pot_ratio,
        pressure_index,
        aggressive_action,
        passive_action,
        check_action,
        fold_action,
        -- アクションの多様性（0-1の値）
        CASE
            WHEN (aggressive_action + passive_action + check_action + fold_action) = 0 THEN 0
            ELSE (
                CASE WHEN aggressive_action > 0 THEN 1 ELSE 0 END +
                CASE WHEN passive_action > 0 THEN 1 ELSE 0 END +
                CASE WHEN check_action > 0 THEN 1 ELSE 0 END +
                CASE WHEN fold_action > 0 THEN 1 ELSE 0 END
            )::FLOAT / 4
        END AS action_diversity
    FROM action_metrics
),

player_action_flags AS (
    SELECT
        hand_id,
        player_id,
        phase_name,
        -- プリフロップアクション指標
        MAX(CASE WHEN phase_name = 'PREFLOP' AND action_name IN ('CALL', 'BET', 'RAISE', 'ALL_IN') THEN 1 ELSE 0 END) as is_vpip,
        MAX(CASE WHEN phase_name = 'PREFLOP' AND action_name IN ('BET', 'RAISE', 'ALL_IN') THEN 1 ELSE 0 END) as is_pfr,
        MAX(CASE WHEN phase_name = 'PREFLOP' AND action_name IN ('RAISE', 'ALL_IN') AND action_order > 1 THEN 1 ELSE 0 END) as is_3bet,
        -- フロップアクション指標（改善版CBET）
        MAX(CASE
            WHEN phase_name = 'FLOP'
            AND action_name IN ('BET', 'RAISE', 'ALL_IN')
            AND EXISTS (
                SELECT 1
                FROM action_diversity pa_pre
                WHERE pa_pre.hand_id = action_diversity.hand_id
                AND pa_pre.player_id = action_diversity.player_id
                AND pa_pre.phase_name = 'PREFLOP'
                AND pa_pre.action_name IN ('BET', 'RAISE', 'ALL_IN')
                AND NOT EXISTS (
                    SELECT 1
                    FROM action_diversity pa_later
                    WHERE pa_later.hand_id = pa_pre.hand_id
                    AND pa_later.player_id = pa_pre.player_id
                    AND pa_later.phase_name = 'PREFLOP'
                    AND pa_later.action_order > pa_pre.action_order
                    AND pa_later.action_name NOT IN ('BET', 'RAISE', 'ALL_IN')
                )
            )
            THEN 1
            ELSE 0
        END) as cbet_flop,
        -- アグレッション指標
        SUM(aggressive_action) as aggressive_actions,
        SUM(passive_action) as passive_actions
    FROM action_diversity
    GROUP BY hand_id, player_id, phase_name
)

SELECT
    hand_id,
    player_id,
    phase_name,
    is_vpip,
    is_pfr,
    is_3bet,
    aggressive_actions,
    passive_actions,
    cbet_flop
FROM player_action_flags
