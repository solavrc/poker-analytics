/*
このモデルは、ポーカーハンドの各フェーズ（PREFLOP, FLOP, TURN, RIVER）における
アクション統計とポット推移を集計します。

主な処理の流れ：
1. フェーズイベントの抽出（phase_events）
   - ハンドイベントからフェーズ情報とポット情報を取得
   - フェーズ名のマッピング（0=PREFLOP, 1=FLOP, 2=TURN, 3=RIVER）
   - フェーズの開始と終了を示すフラグを設定

2. フェーズの境界を特定（phase_boundaries）
   - 各フェーズの開始時と終了時のポットを記録
   - フェーズごとのポット推移を把握

3. フェーズごとのアクション集計（phase_actions）
   - アクションの総数
   - フォールド回数
   - アグレッシブなアクション（BET, RAISE, ALL_IN）の回数
   - アクティブプレイヤー数
   - 合計ベット額

4. 最終的な出力
   - フェーズ情報（phase_name）
   - ポット推移（starting_pot, ending_pot）
   - アクション統計（total_actions, fold_actions, aggressive_actions）
   - プレイヤー情報（active_players）
   - ベット情報（total_bets）
   - アグレッション指標（aggression_frequency）
*/

WITH phase_events AS (
  SELECT
    hand_id,
    event_timestamp,
    phase,
    pot,
    -- フェーズ名のマッピング
    CASE phase
      WHEN 0 THEN 'PREFLOP'
      WHEN 1 THEN 'FLOP'
      WHEN 2 THEN 'TURN'
      WHEN 3 THEN 'RIVER'
    END as phase_name,
    -- フェーズの開始を示すフラグ
    CASE
      WHEN phase != LAG(phase) OVER (PARTITION BY hand_id ORDER BY event_timestamp)
        OR LAG(phase) OVER (PARTITION BY hand_id ORDER BY event_timestamp) IS NULL
      THEN 1
      ELSE 0
    END as is_phase_start,
    -- フェーズの終了を示すフラグ
    CASE
      WHEN phase != LEAD(phase) OVER (PARTITION BY hand_id ORDER BY event_timestamp)
        OR LEAD(phase) OVER (PARTITION BY hand_id ORDER BY event_timestamp) IS NULL
      THEN 1
      ELSE 0
    END as is_phase_end
  FROM {{ ref('stg_hand_events') }}
  WHERE hand_id IS NOT NULL
    AND phase IS NOT NULL  -- フェーズが不明なイベントを除外
),

-- phase_boundaries CTE
-- 目的：各フェーズの開始時と終了時のポットを記録します
-- 処理内容：
-- - フェーズごとに最小ポット（開始時）と最大ポット（終了時）を計算
-- - hand_id, phase_name, phaseでグループ化して集計
phase_boundaries AS (
  SELECT
    hand_id,
    phase_name,
    phase,
    MIN(pot) as starting_pot,  -- フェーズ開始時のポット
    MAX(pot) as ending_pot     -- フェーズ終了時のポット
  FROM phase_events
  WHERE phase_name IS NOT NULL  -- フェーズ名が不明なイベントを除外
  GROUP BY hand_id, phase_name, phase
),

-- phase_actions CTE
-- 目的：フェーズごとのアクション統計を集計します
-- 処理内容：
-- - アクションの総数をカウント
-- - フォールド、アグレッシブアクション（BET, RAISE, ALL_IN）の回数を集計
-- - アクティブプレイヤー数（ユニークなplayer_id）をカウント
-- - 合計ベット額を計算
phase_actions AS (
  SELECT
    hand_id,
    phase,
    COUNT(*) as total_actions,
    SUM(CASE WHEN action_name IN ('FOLD') THEN 1 ELSE 0 END) as fold_actions,
    SUM(CASE WHEN action_name IN ('BET', 'RAISE', 'ALL_IN') THEN 1 ELSE 0 END) as aggressive_actions,
    COUNT(DISTINCT player_id) as active_players,
    SUM(bet_chip) as total_bets
  FROM {{ ref('stg_player_actions') }}
  WHERE hand_id IS NOT NULL
    AND phase IS NOT NULL  -- フェーズが不明なイベントを除外
  GROUP BY hand_id, phase
)

-- 最終的な出力
-- - フェーズ情報（phase_name）とポット推移（starting_pot, ending_pot）
-- - アクション統計（total_actions, fold_actions, aggressive_actions）
-- - プレイヤー情報（active_players）とベット情報（total_bets）
-- - アグレッション指標（aggression_frequency）を計算
SELECT
  pb.hand_id,
  pb.phase_name,
  pb.starting_pot,
  pb.ending_pot,
  COALESCE(pa.total_actions, 0) as total_actions,
  COALESCE(pa.fold_actions, 0) as fold_actions,
  COALESCE(pa.aggressive_actions, 0) as aggressive_actions,
  COALESCE(pa.active_players, 0) as active_players,
  COALESCE(pa.total_bets, 0) as total_bets,
  -- アグレッション頻度（攻撃的なアクションの割合）
  CASE
    WHEN COALESCE(pa.total_actions, 0) > 0 THEN CAST(pa.aggressive_actions AS FLOAT) / pa.total_actions
    ELSE 0
  END as aggression_frequency
FROM phase_boundaries pb
LEFT JOIN phase_actions pa
  ON pb.hand_id = pa.hand_id
  AND pb.phase = pa.phase
ORDER BY pb.hand_id, pb.phase
