/*
このモデルは、ポーカーハンドにおけるプレイヤーのアクション（行動）を処理し、
正規化されたアクションデータを生成します。

主な処理の流れ：
1. アクションイベントの抽出（action_events）
   - EVT_ACTION (304) イベントから必要な情報を取得
   - アクションタイプのマッピング（FOLD, CHECK, CALL, BET, RAISE, ALL_IN）
   - プレイヤーのチップ情報、ベット額、ポット額を取得

2. 最適なアクションの選択（best_actions）
   - 同一hand_idとseat_indexの組み合わせで最も情報量の多いレコードを選択
   - pot, bet_chip, player_chip, next_action_seatの存在を優先度として使用
   - sender_user_idの値で一貫性を確保

3. フェーズ情報の処理（previous_phases, last_actions）
   - 各アクションの前のフェーズを記録
   - 最後のアクション（next_action_seat = -2）を特定
   - フェーズの継承関係を管理

4. 最終的な出力
   - アクションの基本情報（hand_id, event_timestamp, phase）
   - プレイヤー情報（player_id, seat_index）
   - アクション詳細（action_name, action_order）
   - チップ情報（bet_chip, pot, player_chip, bb）
   - データソース情報（source_user_id）
*/

with action_events as (
    select
        hand_id,
        sender_user_id,
        event_timestamp,
        value:SeatIndex::number as seat_index,
        value:Progress:Phase::number as phase,
        case
            when value:ActionType::number = 2 then 'FOLD'
            when value:ActionType::number = 0 then 'CHECK'
            when value:ActionType::number = 3 then 'CALL'
            when value:ActionType::number = 1 then 'BET'
            when value:ActionType::number = 4 then 'RAISE'
            when value:ActionType::number = 5 then 'ALL_IN'
        end as action_name,
        coalesce(value:Chip::number, 0) as player_chip,
        coalesce(value:BetChip::number, 0) as bet_chip,
        coalesce(value:Progress.Pot::number, 0) as pot,
        value:Progress.NextActionSeat::number as next_action_seat
    from {{ ref('stg_hand_events') }}
    where ApiTypeId = 304  -- EVT_ACTION
),

-- best_actions CTE
-- 目的：同一hand_idとseat_indexの組み合わせで最も情報量の多いレコードを選択します
-- 処理内容：
-- - pot, bet_chip, player_chip, next_action_seatの存在を優先度として使用
-- - sender_user_idの値で一貫性を確保
-- - ROW_NUMBER()を使用して各組み合わせで1レコードのみを選択
best_actions as (
    select *
    from action_events
    qualify row_number() over (
        partition by hand_id, event_timestamp, seat_index
        order by
            -- 優先順位:
            -- 1. potが存在する
            case when pot is not null then 1 else 0 end desc,
            -- 2. bet_chipが存在する
            case when bet_chip is not null then 1 else 0 end desc,
            -- 3. player_chipが存在する
            case when player_chip is not null then 1 else 0 end desc,
            -- 4. next_action_seatが存在する
            case when next_action_seat is not null then 1 else 0 end desc,
            -- 5. sender_user_idの値が小さい（一貫性のため）
            sender_user_id asc
    ) = 1
),

-- last_actions CTE
-- 目的：各フェーズの最後のアクションを特定します
-- 処理内容：
-- - next_action_seat = -2 のレコードを抽出（フェーズの最後を示す）
last_actions as (
    select
        hand_id,
        event_timestamp
    from best_actions
    where next_action_seat = -2
),

-- previous_phases CTE
-- 目的：各アクションの前のフェーズを記録します
-- 処理内容：
-- - LAG関数を使用して前のフェーズの値を取得
-- - hand_idでパーティション化してフェーズの継承を管理
previous_phases as (
    select
        a.hand_id,
        a.event_timestamp,
        lag(a.phase) over (
            partition by a.hand_id
            order by a.event_timestamp
        ) as previous_phase
    from best_actions a
),

-- final_actions CTE
-- 目的：アクションの最終的な形式を整えます
-- 処理内容：
-- - フェーズ情報の調整（最後のアクションは前のフェーズを継承）
-- - アクション順序の付与
-- - プレイヤーIDとBB（ビッグブラインド）の関連付け
-- - NULL値の処理（coalesceで0に変換）
final_actions as (
    select
        a.hand_id,
        a.event_timestamp,
        a.seat_index,
        case
            when l.hand_id is not null then coalesce(p.previous_phase, 0)
            else a.phase
        end as phase,
        a.action_name,
        coalesce(a.player_chip, 0) as player_chip,
        coalesce(a.bet_chip, 0) as bet_chip,
        coalesce(a.pot, 0) as pot,
        row_number() over (
            partition by a.hand_id,
            case
                when l.hand_id is not null then coalesce(p.previous_phase, 0)
                else a.phase
            end
            order by a.event_timestamp
        ) as action_order,
        get(h.seat_user_ids, a.seat_index) as player_id,
        h.bb,
        a.sender_user_id as source_user_id  -- 情報の提供元を記録
    from best_actions a
    left join {{ ref('stg_hands') }} h
        on a.hand_id = h.hand_id
    left join last_actions l
        on a.hand_id = l.hand_id
        and a.event_timestamp = l.event_timestamp
    left join previous_phases p
        on a.hand_id = p.hand_id
        and a.event_timestamp = p.event_timestamp
)

-- 最終的な出力
-- - アクションの基本情報（hand_id, event_timestamp, phase）
-- - プレイヤー情報（player_id, seat_index）
-- - アクション詳細（action_name, action_order）
-- - チップ情報（bet_chip, pot, player_chip, bb）
-- - データソース情報（source_user_id）
select
    hand_id,
    event_timestamp,
    phase,
    action_order,
    player_id,
    action_name,
    bet_chip,
    pot,
    player_chip,
    seat_index,
    bb,
    source_user_id
from final_actions
where hand_id is not null
