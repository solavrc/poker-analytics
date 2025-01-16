WITH event_boundaries AS (
  SELECT
    event_timestamp,
    ApiTypeId,
    ApiType,
    HandId as hand_id,
    -- 前のイベントとの時間差を計算
    TIMESTAMPDIFF(SECOND, LAG(event_timestamp) OVER (ORDER BY event_timestamp), event_timestamp) as seconds_from_prev_event,
    -- 前のイベントがEVT_HAND_RESULTSの場合、新しいハンドの開始
    LAG(ApiTypeId) OVER (ORDER BY event_timestamp) as prev_event_type,
    -- 次のEVT_DEALまでを同じハンドとして扱う
    LEAD(ApiTypeId) OVER (ORDER BY event_timestamp) as next_event_type,
    -- 前のEVT_HAND_RESULTSのhand_idを取得
    LAG(HandId) OVER (ORDER BY event_timestamp) as prev_hand_id,
    -- EVT_DEALで、かつ前のEVT_HAND_RESULTSからの最初のEVT_DEALの場合に新しいハンド開始
    CASE
      WHEN ApiTypeId = 303  -- EVT_DEAL
        AND NOT EXISTS (
          SELECT 1
          FROM {{ source('pokerchase', 'raw_api_events') }} prev
          WHERE prev.event_timestamp < event_timestamp
            AND prev.event_timestamp > (
              SELECT MAX(e2.event_timestamp)
              FROM {{ source('pokerchase', 'raw_api_events') }} e2
              WHERE e2.event_timestamp < event_timestamp
                AND e2.ApiTypeId = 306  -- EVT_HAND_RESULTS
            )
            AND prev.ApiTypeId = 303  -- EVT_DEAL
        )
      THEN 1
      ELSE 0
    END as is_new_hand
  FROM {{ source('pokerchase', 'raw_api_events') }}
  WHERE ApiTypeId IN (303, 304, 305, 306)  -- 関連するイベントのみを対象とする
),
hand_groups AS (
  SELECT
    event_timestamp,
    ApiTypeId,
    ApiType,
    hand_id,
    seconds_from_prev_event,
    prev_event_type,
    next_event_type,
    prev_hand_id,
    is_new_hand,
    -- ハンドの開始からの連番を付与
    SUM(is_new_hand) OVER (ORDER BY event_timestamp) as hand_group
  FROM event_boundaries
),
-- ハンドごとのイベント間隔チェック
hand_validity AS (
  SELECT
    hg.hand_group,
    -- 30秒を超えるイベント間隔が存在するかチェック
    MAX(CASE
      WHEN hg.seconds_from_prev_event > 20
      AND hg.ApiTypeId = 304  -- EVT_ACTIONの場合のみチェック
      THEN 1
      ELSE 0
    END) as has_invalid_interval,
    -- 最大のイベント間隔
    MAX(CASE
      WHEN hg.ApiTypeId = 304  -- EVT_ACTIONの場合のみ
      THEN hg.seconds_from_prev_event
      ELSE 0
    END) as max_action_interval
  FROM hand_groups hg
  GROUP BY hg.hand_group
),
hand_ids AS (
  SELECT
    hg.event_timestamp,
    hg.ApiTypeId,
    hg.ApiType,
    hg.hand_id,
    hg.seconds_from_prev_event,
    hg.prev_event_type,
    hg.next_event_type,
    hg.prev_hand_id,
    hg.is_new_hand,
    hg.hand_group,
    v.has_invalid_interval,
    v.max_action_interval,
    -- 同じhand_group内での最初のhand_id
    FIRST_VALUE(hg.hand_id IGNORE NULLS) OVER (
      PARTITION BY hg.hand_group
      ORDER BY hg.event_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as first_hand_id,
    -- 同じhand_group内での最後のhand_id
    LAST_VALUE(hg.hand_id IGNORE NULLS) OVER (
      PARTITION BY hg.hand_group
      ORDER BY hg.event_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as last_hand_id
  FROM hand_groups hg
  JOIN hand_validity v ON v.hand_group = hg.hand_group
),
hand_boundaries AS (
  SELECT
    event_timestamp,
    ApiTypeId,
    ApiType,
    hand_group,
    is_new_hand,
    has_invalid_interval,
    max_action_interval,
    -- ハンドIDの付与ロジック
    CASE
      WHEN has_invalid_interval = 0 THEN  -- 有効なハンドの場合のみハンドIDを付与
        COALESCE(
          first_hand_id,
          CASE
            WHEN ApiTypeId = 306 THEN hand_id
            ELSE last_hand_id
          END
        )
      ELSE NULL  -- 無効なハンドの場合はNULL
    END as hand_id,
    -- ハンドの開始フラグ
    CASE
      WHEN ApiTypeId = 303 AND is_new_hand = 1 THEN 1
      ELSE 0
    END as is_hand_start,
    -- ハンドの終了フラグ
    CASE
      WHEN ApiTypeId = 306 THEN 1
      ELSE 0
    END as is_hand_end,
    hand_group as hand_sequence
  FROM hand_ids
)
SELECT
  s.event_timestamp,
  e.ApiTypeId,
  e.ApiType,
  s.hand_sequence,
  s.is_hand_start,
  s.is_hand_end,
  s.hand_id,
  s.has_invalid_interval,  -- 無効なインターバルの有無
  s.max_action_interval,   -- 最大アクション間隔
  e.Phase,
  e.ActionType,
  e.BetChip,
  COALESCE(e.Pot, 0) as Pot,
  e.Chip,
  e.SeatUserIds,
  e.SeatIndex,
  e.NextActionSeat,
  e.ButtonSeat,
  e.SmallBlindSeat,
  e.BigBlindSeat,
  e.SmallBlind,
  e.BigBlind,
  e.Ante,
  e.CurrentBlindLv,
  e.NextBlindUnixSeconds,
  e.HoleCards,
  e.CommunityCards,
  e.Results,
  e.VALUE,
FROM hand_boundaries s
JOIN {{ source('pokerchase', 'raw_api_events') }} e
  ON s.event_timestamp = e.event_timestamp
  AND e.ApiTypeId IN (303, 304, 305, 306)  -- 関連するイベントのみを対象とする
WHERE hand_sequence > 0  -- 最初のEVT_DEAL以前のイベントを除外
  AND s.has_invalid_interval = 0  -- 有効なハンドのみを対象とする
