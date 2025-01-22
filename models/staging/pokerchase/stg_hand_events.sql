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
with
    -- session_boundaries CTE
    -- 目的：セッションの境界を特定し、セッションIDを割り当てます
    -- 処理内容：
    -- - EVT_SESSION_DETAILS (308) から次の EVT_SESSION_DETAILS または EVT_SESSION_RESULTS (309) までを追跡
    -- - セッション開始時のタイムスタンプをセッションIDとして使用
    session_groups as (
        select e.*, sum(case when api_type_id = 308 then 1 else 0 end) over (partition by sender_user_id order by event_timestamp) as session_group
        from {{ source("pokerchase", "raw_api_events") }} e
        where api_type_id in (303, 304, 305, 306, 308, 309)
    ),

    session_boundaries as (
        select
            sg.*,
            first_value(event_timestamp) over (partition by sender_user_id, session_group order by event_timestamp) as session_start_time,
            {{ dbt_utils.generate_surrogate_key(["sender_user_id", "session_group"]) }} as session_id
        from session_groups sg
    ),

    -- event_boundaries CTE
    -- 目的：ハンドに関連するイベントを抽出し、各イベントの前後関係を把握します
    -- 処理内容：
    -- - ハンドイベント（303-306）のみを対象とする
    -- - 各イベントの前後のイベントタイプとタイムスタンプを取得
    -- - イベント間隔が60秒以内であることを確認
    event_boundaries as (
        select
            sb.*,
            lag(event_timestamp) over (partition by sender_user_id order by event_timestamp) as prev_event_timestamp,
            lag(api_type_id) over (partition by sender_user_id order by event_timestamp) as prev_event_type,
            lead(event_timestamp) over (partition by sender_user_id order by event_timestamp) as next_event_timestamp,
            lead(api_type_id) over (partition by sender_user_id order by event_timestamp) as next_event_type,
            -- 前のイベントとの間隔チェック
            timestampdiff(second, lag(event_timestamp) over (partition by sender_user_id order by event_timestamp), event_timestamp) as seconds_from_prev,
            -- 次のイベントとの間隔チェック
            timestampdiff(second, event_timestamp, lead(event_timestamp) over (partition by sender_user_id order by event_timestamp)) as seconds_to_next,
            -- 60秒以上の間隔があるイベントを検出
            case
                when
                    timestampdiff(second, lag(event_timestamp) over (partition by sender_user_id order by event_timestamp), event_timestamp) > 60
                    or timestampdiff(second, event_timestamp, lead(event_timestamp) over (partition by sender_user_id order by event_timestamp)) > 60
                then 1
                else 0
            end as has_invalid_interval
        from session_boundaries sb
        where api_type_id in (303, 304, 305, 306)
    ),

    -- hand_groups CTE
    -- 目的：同一ハンドに属するイベントをグループ化します
    -- 処理内容：
    -- - EVT_DEAL (303) を起点に昇順でイベントをグループ化
    -- - EVT_HAND_RESULTS (306) が EVT_DEAL の前に出現する場合、それは前のハンドの終了
    -- - hand_group により、同一ハンドのイベントに同じ値を付与
    hand_groups as (
        select
            eb.*,
            -- 新しいハンドグループの開始を検出
            sum(case when api_type_id = 303 then 1 else 0 end) over (partition by sender_user_id order by event_timestamp) as hand_group,
            -- イベントの有効性を判定
            case
                when api_type_id = 303
                then 1  -- EVT_DEAL は常に有効
                when api_type_id = 306
                then 1  -- EVT_HAND_RESULTS は常に有効
                when lag(api_type_id) over (partition by sender_user_id order by event_timestamp) = 306 and api_type_id != 303
                then 0  -- EVT_HAND_RESULTS の後の非EVT_DEALイベントは無効
                else 1  -- その他のイベントは有効
            end as is_valid_event
        from event_boundaries eb
        where has_invalid_interval = 0  -- 60秒以上の間隔があるイベントを除外
    ),

    -- hand_validity CTE
    -- 目的：ハンドの有効性を確認し、正しいHandIdを割り当てます
    -- 処理内容：
    -- - EVT_HAND_RESULTS (306) から HandId を取得し、同一グループ内の全イベントに適用
    -- - EVT_DEAL (303) の存在を確認（has_deal_event）
    -- - 同一hand_group内のイベントに同じHandIdを付与
    hand_validity as (
        select
            -- exclude: (42601): SQL compilation error: ambiguous column name 'HAND_ID'
            hg.* exclude(hand_id),
            -- 同一グループ内の最後のEVT_HAND_RESULTSからHandIdを取得
            last_value(case when api_type_id = 306 then hand_id else null end ignore nulls) over (
                partition by sender_user_id, hand_group order by event_timestamp rows between unbounded preceding and unbounded following
            ) as hand_id,
            -- EVT_DEALの存在確認
            max(case when api_type_id = 303 then 1 else 0 end) over (partition by sender_user_id, hand_group) as has_deal_event,
            -- EVT_HAND_RESULTSの存在確認
            max(case when api_type_id = 306 then 1 else 0 end) over (partition by sender_user_id, hand_group) as has_results_event
        from hand_groups hg
    )

-- 最終的な出力
-- - 有効なハンドイベントのみを抽出（has_deal_event = 1 AND has_results_event = 1）
-- - イベントの基本情報（タイムスタンプ、送信者、ハンドID、イベントタイプ）
-- - セッション情報（session_id, session_group, session_start_time）
-- - 時間間隔情報（seconds_from_prev, seconds_to_next）
-- - 生データ（value）を含む
-- - ハンドの開始（EVT_DEAL）と終了（EVT_HAND_RESULTS）を示すフラグ
select
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
    case when hv.api_type_id = 303 then 1 else 0 end as is_hand_start,
    case when hv.api_type_id = 306 then 1 else 0 end as is_hand_end,
    -- ハンドシーケンスを追加（同一ハンド内でのイベントの順序）
    row_number() over (partition by hv.hand_id order by hv.event_timestamp) as hand_sequence,
    -- フェーズ情報を追加
    case when hv.api_type_id = 304 then hv.progress:"Phase"::number when hv.api_type_id = 305 then hv.progress:"Phase"::number else null end as phase,
    -- ポット情報を追加
    case when hv.api_type_id = 304 then hv.progress:"Pot"::number when hv.api_type_id = 305 then hv.progress:"Pot"::number else null end as pot
from hand_validity hv
where
    hv.has_deal_event = 1  -- EVT_DEALが存在するハンドのみを対象とする
    and hv.has_results_event = 1  -- EVT_HAND_RESULTSが存在するハンドのみを対象とする
    and hv.is_valid_event = 1  -- 無効なイベントを除外
    and hv.hand_id is not null  -- HandIdが取得できたハンドのみを対象とする
order by hv.event_timestamp
