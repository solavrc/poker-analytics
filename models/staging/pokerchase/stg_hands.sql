with deal_events as (
    select
        hand_id,
        ButtonSeat as button_seat,
        SmallBlindSeat as sb_seat,
        BigBlindSeat as bb_seat,
        SmallBlind as sb,
        BigBlind as bb,
        Ante as ante,
        CurrentBlindLv as current_blind_lv,
        CASE
            WHEN NextBlindUnixSeconds = -1 THEN 'RING_GAME'
            ELSE 'TOURNAMENT'
        END as game_type,
        NextBlindUnixSeconds as next_blind_unix_seconds,
        SeatUserIds as seat_user_ids,
        event_timestamp as start_timestamp
    from {{ ref('stg_hand_events') }}
    where ApiTypeId = 303  -- EVT_DEAL
    and is_hand_start = 1  -- ハンドの開始時のみ
),

hand_results as (
    select
        hand_id,
        event_timestamp as end_timestamp
    from {{ ref('stg_hand_events') }}
    where ApiTypeId = 306  -- EVT_HAND_RESULTS
    qualify row_number() over (
        partition by hand_id
        order by event_timestamp desc
    ) = 1
),

flop_cards as (
    select
        hand_id,
        CommunityCards as cards
    from {{ ref('stg_hand_events') }}
    where ApiTypeId = 305  -- EVT_DEAL_ROUND
    and Phase = 1
    and CommunityCards is not null
    qualify row_number() over (
        partition by hand_id
        order by event_timestamp desc
    ) = 1
),

turn_cards as (
    select
        hand_id,
        CommunityCards as cards
    from {{ ref('stg_hand_events') }}
    where ApiTypeId = 305  -- EVT_DEAL_ROUND
    and Phase = 2
    and CommunityCards is not null
    qualify row_number() over (
        partition by hand_id
        order by event_timestamp desc
    ) = 1
),

river_cards as (
    select
        hand_id,
        CommunityCards as cards
    from {{ ref('stg_hand_events') }}
    where ApiTypeId = 305  -- EVT_DEAL_ROUND
    and Phase = 3
    and CommunityCards is not null
    qualify row_number() over (
        partition by hand_id
        order by event_timestamp desc
    ) = 1
),

result_cards as (
    select
        hand_id,
        CommunityCards as cards
    from {{ ref('stg_hand_events') }}
    where ApiTypeId = 306  -- EVT_HAND_RESULTS
    and CommunityCards is not null
    and array_size(CommunityCards) > 0  -- 空配列を除外
    qualify row_number() over (
        partition by hand_id
        order by event_timestamp desc
    ) = 1
),

combined_cards as (
    select distinct
        d.hand_id,
        coalesce(
            -- 通常のフェーズ進行の場合
            case when array_size(array_cat(
                array_cat(
                    coalesce(f.cards, array_construct()),
                    coalesce(t.cards, array_construct())
                ),
                coalesce(r.cards, array_construct())
            )) > 0
            then array_cat(
                array_cat(
                    coalesce(f.cards, array_construct()),
                    coalesce(t.cards, array_construct())
                ),
                coalesce(r.cards, array_construct())
            )
            -- プリフロップオールインの場合
            else res.cards
            end,
            array_construct()
        ) as community_cards
    from deal_events d
    left join flop_cards f on d.hand_id = f.hand_id
    left join turn_cards t on d.hand_id = t.hand_id
    left join river_cards r on d.hand_id = r.hand_id
    left join result_cards res on d.hand_id = res.hand_id
),

card_strings as (
    select
        c.hand_id,
        c.community_cards,
        {{ convert_card_array_to_strings('value') }} as community_cards_str
    from combined_cards c
    cross join table(flatten(input => c.community_cards)) as cards
    group by c.hand_id, c.community_cards
)

select
    d.hand_id,
    d.start_timestamp,
    hr.end_timestamp,
    d.game_type,
    c.community_cards,
    c.community_cards_str,
    d.seat_user_ids,
    d.button_seat,
    d.sb_seat,
    d.bb_seat,
    d.sb,
    d.bb,
    d.ante,
    d.current_blind_lv,
from deal_events d
left join card_strings c
    on d.hand_id = c.hand_id
left join hand_results hr
    on d.hand_id = hr.hand_id
where d.hand_id is not null
