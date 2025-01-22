WITH source AS (
    SELECT *
    FROM {{ source('pokerchase', 'raw_api_events') }}
    WHERE ApiTypeId = 306  -- EVT_HAND_RESULTS
),

renamed AS (
    SELECT
        HandId as hand_id,
        sender_user_id,
        r.VALUE:UserId::NUMBER as player_id,
        r.VALUE:HoleCards::ARRAY as hole_cards,
        r.VALUE:RankType::NUMBER as rank_type,
        r.VALUE:HandRanking::NUMBER as hand_ranking,
        r.VALUE:Ranking::NUMBER as ranking,
        r.VALUE:RewardChip::NUMBER as reward_chip
    FROM source,
    LATERAL FLATTEN(input => Results) r
    WHERE r.VALUE:UserId IS NOT NULL  -- NULLのプレイヤーを除外
),

-- 同一hand_idとplayer_idの組み合わせで最も多くの情報を持つレコードを選択
best_results as (
    select *
    from renamed
    qualify row_number() over (
        partition by hand_id, player_id
        order by
            -- 優先順位:
            -- 1. hole_cardsの要素数が多い（より多くのカード情報を持つ）
            array_size(hole_cards) desc nulls last,
            -- 2. reward_chipが存在する
            case when reward_chip is not null then 1 else 0 end desc,
            -- 3. rank_typeが存在する
            case when rank_type is not null then 1 else 0 end desc,
            -- 4. sender_user_idがplayer_idと一致（自分自身のログを優先）
            case when sender_user_id = player_id then 1 else 0 end desc,
            -- 5. sender_user_idの値が小さい（一貫性のため）
            sender_user_id asc
    ) = 1
),

empty_cards as (
    select
        hand_id,
        player_id,
        hole_cards,
        []::ARRAY as hole_cards_str
    from best_results
    where ARRAY_SIZE(hole_cards) = 0
),

non_empty_cards as (
    select
        r.hand_id,
        r.player_id,
        r.hole_cards,
        {{ convert_card_array_to_strings('value') }} as hole_cards_str
    from best_results r
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
    r.reward_chip,
    r.sender_user_id as source_user_id  -- 情報の提供元を記録
FROM best_results r
LEFT JOIN card_strings c
    ON r.hand_id = c.hand_id
    AND r.player_id = c.player_id
ORDER BY r.hand_id, r.player_id
