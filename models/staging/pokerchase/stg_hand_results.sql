with
    source as (
        select * from {{ source("pokerchase", "raw_api_events") }} where api_type_id = 306  -- EVT_HAND_RESULTS
    ),

    renamed as (
        select
            s.hand_id,
            s.sender_user_id,
            r.value:"UserId"::number as player_id,
            r.value:"HoleCards"::array as hole_cards,
            r.value:"RankType"::number as rank_type,
            r.value:"HandRanking"::number as hand_ranking,
            r.value:"Ranking"::number as ranking,
            r.value:"RewardChip"::number as reward_chip
        from source s, lateral flatten(input => s.results) r
        where r.value:"UserId" is not null  -- NULLのプレイヤーを除外
    ),

    -- 同一hand_idとplayer_idの組み合わせで最も多くの情報を持つレコードを選択
    best_results as (
        select *
        from renamed
        qualify
            row_number() over (
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
            )
            = 1
    ),

    empty_cards as (select hand_id, player_id, hole_cards, []::array as hole_cards_str from best_results where array_size(hole_cards) = 0),

    non_empty_cards as (
        select r.hand_id, r.player_id, r.hole_cards, {{ convert_card_array_to_strings("value") }} as hole_cards_str
        from best_results r
        cross join table(flatten(input => r.hole_cards)) as cards
        where array_size(r.hole_cards) > 0
        group by r.hand_id, r.player_id, r.hole_cards
    ),

    card_strings as (
        select *
        from empty_cards
        union all
        select *
        from non_empty_cards
    )

select r.hand_id, r.player_id, r.hole_cards, c.hole_cards_str, r.rank_type, r.hand_ranking, r.ranking, r.reward_chip, r.sender_user_id as source_user_id  -- 情報の提供元を記録
from best_results r
left join card_strings c on r.hand_id = c.hand_id and r.player_id = c.player_id
order by r.hand_id, r.player_id
