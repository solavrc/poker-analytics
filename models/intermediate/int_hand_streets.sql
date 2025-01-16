WITH phase_events AS (
  SELECT
    hand_id,
    event_timestamp,
    Phase,
    Pot,
    -- フェーズ名のマッピング
    CASE Phase
      WHEN 0 THEN 'PREFLOP'
      WHEN 1 THEN 'FLOP'
      WHEN 2 THEN 'TURN'
      WHEN 3 THEN 'RIVER'
    END as phase_name,
    -- フェーズの開始を示すフラグ
    CASE
      WHEN Phase != LAG(Phase) OVER (PARTITION BY hand_id ORDER BY event_timestamp)
        OR LAG(Phase) OVER (PARTITION BY hand_id ORDER BY event_timestamp) IS NULL
      THEN 1
      ELSE 0
    END as is_phase_start,
    -- フェーズの終了を示すフラグ
    CASE
      WHEN Phase != LEAD(Phase) OVER (PARTITION BY hand_id ORDER BY event_timestamp)
        OR LEAD(Phase) OVER (PARTITION BY hand_id ORDER BY event_timestamp) IS NULL
      THEN 1
      ELSE 0
    END as is_phase_end
  FROM {{ ref('stg_hand_events') }}
  WHERE hand_id IS NOT NULL
    AND Phase IS NOT NULL  -- フェーズが不明なイベントを除外
),
phase_boundaries AS (
  SELECT
    hand_id,
    phase_name,
    Phase,
    MIN(Pot) as starting_pot,  -- フェーズ開始時のポット
    MAX(Pot) as ending_pot     -- フェーズ終了時のポット
  FROM phase_events
  WHERE phase_name IS NOT NULL  -- フェーズ名が不明なイベントを除外
  GROUP BY hand_id, phase_name, Phase
),
phase_actions AS (
  SELECT
    hand_id,
    CASE phase
      WHEN 0 THEN 'PREFLOP'
      WHEN 1 THEN 'FLOP'
      WHEN 2 THEN 'TURN'
      WHEN 3 THEN 'RIVER'
    END as phase_name,
    COUNT(DISTINCT player_id) as active_players,
    COUNT(*) as total_actions,
    COUNT(CASE WHEN action_name IN ('BET', 'RAISE') THEN 1 END) as aggressive_actions,
    COUNT(CASE WHEN action_name = 'FOLD' THEN 1 END) as fold_actions,
    SUM(bet_chip) as total_bets
  FROM {{ ref('stg_player_actions') }}
  GROUP BY hand_id, phase
)

SELECT
  pb.hand_id,
  pb.phase_name,
  COALESCE(pb.starting_pot, 0) as starting_pot,  -- NULLの場合は0を設定
  COALESCE(pb.ending_pot, pb.starting_pot, 0) as ending_pot,  -- NULLの場合は開始時のポットまたは0を設定
  -- アクション情報
  COALESCE(pa.active_players, 0) as active_players,
  COALESCE(pa.total_actions, 0) as total_actions,
  COALESCE(pa.aggressive_actions, 0) as aggressive_actions,
  COALESCE(pa.fold_actions, 0) as fold_actions,
  COALESCE(pa.total_bets, 0) as total_bets,
  -- アクション指標
  CASE
    WHEN COALESCE(pa.total_actions, 0) > 0 THEN pa.aggressive_actions::FLOAT / pa.total_actions
    ELSE 0
  END as aggression_frequency
FROM phase_boundaries pb
LEFT JOIN phase_actions pa
  ON pb.hand_id = pa.hand_id
  AND pb.phase_name = pa.phase_name
