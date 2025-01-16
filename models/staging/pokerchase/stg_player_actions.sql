with action_events as (
    select
        hand_id,
        event_timestamp,
        SeatIndex as seat_index,
        Phase as phase,
        case
            when ActionType = 2 then 'FOLD'
            when ActionType = 0 then 'CHECK'
            when ActionType = 3 then 'CALL'
            when ActionType = 1 then 'BET'
            when ActionType = 4 then 'RAISE'
            when ActionType = 5 then 'ALL_IN'
        end as action_name,
        Chip as player_chip,
        BetChip as bet_chip,
        Pot as pot,
        NextActionSeat as next_action_seat
    from {{ ref('stg_hand_events') }}
    where ApiTypeId = 304  -- EVT_ACTION
),

last_actions as (
    select
        hand_id,
        event_timestamp
    from action_events
    where next_action_seat = -2
),

previous_phases as (
    select
        a.hand_id,
        a.event_timestamp,
        lag(a.phase) over (
            partition by a.hand_id
            order by a.event_timestamp
        ) as previous_phase
    from action_events a
),

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
        a.player_chip,
        a.bet_chip,
        a.pot,
        row_number() over (
            partition by a.hand_id,
            case
                when l.hand_id is not null then coalesce(p.previous_phase, 0)
                else a.phase
            end
            order by a.event_timestamp
        ) as action_order,
        get(h.seat_user_ids, a.seat_index) as player_id,
        h.bb
    from action_events a
    left join {{ ref('stg_hands') }} h
        on a.hand_id = h.hand_id
    left join last_actions l
        on a.hand_id = l.hand_id
        and a.event_timestamp = l.event_timestamp
    left join previous_phases p
        on a.hand_id = p.hand_id
        and a.event_timestamp = p.event_timestamp
)

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
    bb
from final_actions
where hand_id is not null
