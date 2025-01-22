/*
このモデルは、ポーカーハンドのイベントを処理し、有効なイベントシーケンスを抽出します。

主な処理の流れ：
1. セッションの境界を特定（session_boundaries）
   - EVT_SESSION_DETAILS (308) から次の EVT_SESSION_DETAILS/RESULTS までを同一セッションとして扱う
   - 最初の HandId をセッションIDとして使用

2. イベントの境界を特定（event_boundaries）
   - ハンドに関連するイベント（303-306）を抽出
   - 各イベントの前後関係を把握
   - イベント間隔が60秒以内であることを確認

3. ハンドグループを作成（hand_groups）
   - EVT_DEAL (303) を起点に昇順で EVT_HAND_RESULTS (306) までを同一ハンドとしてグループ化
   - 無効なシーケンス（EVT_HAND_RESULTS が EVT_DEAL の前に出現）を除外

4. ハンドの有効性をチェック（hand_validity）
   - EVT_HAND_RESULTS (306) から HandId を取得し、同一グループ内の全イベントに適用
   - EVT_DEAL (303) の存在を確認（has_deal_event）
   - 同一hand_group内のイベントに同じHandIdを付与
*/

WITH
-- session_boundaries CTE
-- 目的：セッションの境界を特定し、セッションIDを割り当てます
-- 処理内容：
-- - EVT_SESSION_DETAILS (308) から次の EVT_SESSION_DETAILS または EVT_SESSION_RESULTS (309) までを追跡
-- - セッション開始時のタイムスタンプをセッションIDとして使用
session_groups AS (
    SELECT
        e.*,
        SUM(CASE WHEN api_type_id = 308 THEN 1 ELSE 0 END)
            OVER (PARTITION BY sender_user_id ORDER BY event_timestamp) as session_group
    FROM {{ source('pokerchase', 'raw_api_events') }} e
    WHERE api_type_id IN (303, 304, 305, 306, 308, 309)
),

session_boundaries AS (
    SELECT
        sg.*,
        FIRST_VALUE(event_timestamp) OVER (
            PARTITION BY sender_user_id, session_group
            ORDER BY event_timestamp
        ) as session_start_time,
        {{ dbt_utils.generate_surrogate_key(['sender_user_id', 'session_group']) }} as session_id
    FROM session_groups sg
),

-- event_boundaries CTE
-- 目的：ハンドに関連するイベントを抽出し、各イベントの前後関係を把握します
-- 処理内容：
-- - ハンドイベント（303-306）のみを対象とする
-- - 各イベントの前後のイベントタイプとタイムスタンプを取得
-- - イベント間隔が60秒以内であることを確認
event_boundaries AS (
    SELECT
        sb.*,
        LAG(event_timestamp) OVER (PARTITION BY sender_user_id ORDER BY event_timestamp) as prev_event_timestamp,
        LAG(api_type_id) OVER (PARTITION BY sender_user_id ORDER BY event_timestamp) as prev_event_type,
        LEAD(event_timestamp) OVER (PARTITION BY sender_user_id ORDER BY event_timestamp) as next_event_timestamp,
        LEAD(api_type_id) OVER (PARTITION BY sender_user_id ORDER BY event_timestamp) as next_event_type,
        -- 前のイベントとの間隔チェック
        TIMESTAMPDIFF(SECOND, LAG(event_timestamp) OVER (PARTITION BY sender_user_id ORDER BY event_timestamp), event_timestamp) as seconds_from_prev,
        -- 次のイベントとの間隔チェック
        TIMESTAMPDIFF(SECOND, event_timestamp, LEAD(event_timestamp) OVER (PARTITION BY sender_user_id ORDER BY event_timestamp)) as seconds_to_next,
        -- 60秒以上の間隔があるイベントを検出
        CASE WHEN
            TIMESTAMPDIFF(SECOND, LAG(event_timestamp) OVER (PARTITION BY sender_user_id ORDER BY event_timestamp), event_timestamp) > 60
            OR TIMESTAMPDIFF(SECOND, event_timestamp, LEAD(event_timestamp) OVER (PARTITION BY sender_user_id ORDER BY event_timestamp)) > 60
            THEN 1 ELSE 0
        END as has_invalid_interval
    FROM session_boundaries sb
    WHERE api_type_id IN (303, 304, 305, 306)
),

-- hand_groups CTE
-- 目的：同一ハンドに属するイベントをグループ化します
-- 処理内容：
-- - EVT_DEAL (303) を起点に昇順でイベントをグループ化
-- - EVT_HAND_RESULTS (306) が EVT_DEAL の前に出現する場合、それは前のハンドの終了
-- - hand_group により、同一ハンドのイベントに同じ値を付与
hand_groups AS (
    SELECT
        eb.*,
        -- 新しいハンドグループの開始を検出
        SUM(CASE WHEN api_type_id = 303 THEN 1 ELSE 0 END)
            OVER (PARTITION BY sender_user_id ORDER BY event_timestamp) as hand_group,
        -- イベントの有効性を判定
        CASE
            WHEN api_type_id = 303 THEN 1  -- EVT_DEAL は常に有効
            WHEN api_type_id = 306 THEN 1  -- EVT_HAND_RESULTS は常に有効
            WHEN LAG(api_type_id) OVER (PARTITION BY sender_user_id ORDER BY event_timestamp) = 306
                AND api_type_id != 303 THEN 0  -- EVT_HAND_RESULTS の後の非EVT_DEALイベントは無効
            ELSE 1  -- その他のイベントは有効
        END as is_valid_event
    FROM event_boundaries eb
    WHERE has_invalid_interval = 0  -- 60秒以上の間隔があるイベントを除外
),

-- hand_validity CTE
-- 目的：ハンドの有効性を確認し、正しいHandIdを割り当てます
-- 処理内容：
-- - EVT_HAND_RESULTS (306) から HandId を取得し、同一グループ内の全イベントに適用
-- - EVT_DEAL (303) の存在を確認（has_deal_event）
-- - 同一hand_group内のイベントに同じHandIdを付与
hand_validity AS (
    SELECT
        -- exclude: (42601): SQL compilation error: ambiguous column name 'HAND_ID'
        hg.* exclude(hand_id),
        -- 同一グループ内の最後のEVT_HAND_RESULTSからHandIdを取得
        LAST_VALUE(CASE WHEN api_type_id = 306 THEN hand_id ELSE NULL END IGNORE NULLS)
            OVER (PARTITION BY sender_user_id, hand_group ORDER BY event_timestamp
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as hand_id,
        -- EVT_DEALの存在確認
        MAX(CASE WHEN api_type_id = 303 THEN 1 ELSE 0 END)
            OVER (PARTITION BY sender_user_id, hand_group) as has_deal_event,
        -- EVT_HAND_RESULTSの存在確認
        MAX(CASE WHEN api_type_id = 306 THEN 1 ELSE 0 END)
            OVER (PARTITION BY sender_user_id, hand_group) as has_results_event
    FROM hand_groups hg
)

-- 最終的な出力
-- - 有効なハンドイベントのみを抽出（has_deal_event = 1 AND has_results_event = 1）
-- - イベントの基本情報（タイムスタンプ、送信者、ハンドID、イベントタイプ）
-- - セッション情報（session_id, session_group, session_start_time）
-- - 時間間隔情報（seconds_from_prev, seconds_to_next）
-- - 生データ（value）を含む
-- - ハンドの開始（EVT_DEAL）と終了（EVT_HAND_RESULTS）を示すフラグ
SELECT
    hv.event_timestamp,
    hv.sender_user_id,
    hv.session_id,
    hv.session_group,
    hv.session_start_time,
    hv.hand_id,
    hv.api_type_id,
    hv.seconds_from_prev,
    hv.seconds_to_next,
    hv.has_invalid_interval,
    hv.value,
    CASE WHEN hv.api_type_id = 303 THEN 1 ELSE 0 END as is_hand_start,
    CASE WHEN hv.api_type_id = 306 THEN 1 ELSE 0 END as is_hand_end,
    -- ハンドシーケンスを追加（同一ハンド内でのイベントの順序）
    ROW_NUMBER() OVER (PARTITION BY hv.hand_id ORDER BY hv.event_timestamp) as hand_sequence,
    -- フェーズ情報を追加
    CASE
        WHEN hv.api_type_id = 304 THEN hv.progress:"Phase"::number
        WHEN hv.api_type_id = 305 THEN hv.progress:"Phase"::number
        ELSE NULL
    END as phase,
    -- ポット情報を追加
    CASE
        WHEN hv.api_type_id = 304 THEN hv.progress:"Pot"::number
        WHEN hv.api_type_id = 305 THEN hv.progress:"Pot"::number
        ELSE NULL
    END as pot
FROM hand_validity hv
WHERE hv.has_deal_event = 1  -- EVT_DEALが存在するハンドのみを対象とする
AND hv.has_results_event = 1  -- EVT_HAND_RESULTSが存在するハンドのみを対象とする
AND hv.is_valid_event = 1  -- 無効なイベントを除外
AND hv.hand_id IS NOT NULL  -- HandIdが取得できたハンドのみを対象とする
ORDER BY hv.event_timestamp
