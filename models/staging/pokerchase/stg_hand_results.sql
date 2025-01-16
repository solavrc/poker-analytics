WITH source AS (
    SELECT *
    FROM {{ source('pokerchase', 'raw_api_events') }}
    WHERE ApiTypeId = 306  -- EVT_HAND_RESULTS
),

renamed AS (
    SELECT
        HandId as hand_id,
        r.VALUE:UserId::NUMBER as player_id,
        r.VALUE:HoleCards::ARRAY as hole_cards,
        r.VALUE:RankType::NUMBER as rank_type,
        r.VALUE:HandRanking::NUMBER as hand_ranking,
        r.VALUE:Ranking::NUMBER as ranking,
        r.VALUE:RewardChip::NUMBER as reward_chip
    FROM source,
    LATERAL FLATTEN(input => Results) r
),

empty_cards as (
    select
        hand_id,
        player_id,
        hole_cards,
        []::ARRAY as hole_cards_str
    from renamed
    where ARRAY_SIZE(hole_cards) = 0
),

non_empty_cards as (
    select
        r.hand_id,
        r.player_id,
        r.hole_cards,
        {{ convert_card_array_to_strings('value') }} as hole_cards_str
    from renamed r
    cross join table(flatten(input => r.hole_cards)) as cards
    where ARRAY_SIZE(r.hole_cards) > 0
    group by r.hand_id, r.player_id, r.hole_cards
),

card_strings as (
    select * from empty_cards
    union all
    select * from non_empty_cards
)

SELECT
    r.hand_id,
    r.player_id,
    r.hole_cards,
    c.hole_cards_str,
    r.rank_type,
    r.hand_ranking,
    r.ranking,
    r.reward_chip
FROM renamed r
LEFT JOIN card_strings c
    ON r.hand_id = c.hand_id
    AND r.player_id = c.player_id
