with
    player_actions_base as (
        select
            hand_id,
            player_id,
            phase,
            action_name,
            action_order,
            bet_chip,
            player_chip,
            lag(action_name) over (
                partition by hand_id order by action_order
            ) as prev_action_name,
            lag(player_id) over (
                partition by hand_id order by action_order
            ) as prev_player_id
        from {{ ref("stg_player_actions") }}
    ),

    player_actions as (
        select
            hand_id,
            player_id,
            -- プリフロップのアクション指標
            max(
                case
                    when phase = 0 and action_name in ('BET', 'RAISE', 'CALL')
                    then 1
                    else 0
                end
            ) as is_vpip,
            max(
                case
                    when phase = 0 and action_name in ('BET', 'RAISE') then 1 else 0
                end
            ) as is_pfr,
            max(
                case
                    when
                        phase = 0
                        and action_name = 'RAISE'
                        and prev_action_name = 'RAISE'
                    then 1
                    else 0
                end
            ) as is_3bet,
            -- フロップのアクション指標
            max(
                case
                    when
                        phase = 1
                        and action_name in ('BET', 'RAISE')
                        and exists (
                            select 1
                            from {{ ref("stg_player_actions") }} pa2
                            where
                                pa2.hand_id = pa1.hand_id
                                and pa2.player_id = pa1.player_id
                                and pa2.phase = 0
                                and pa2.action_name in ('BET', 'RAISE')
                        )
                    then 1
                    else 0
                end
            ) as cbet_flop,
            -- CBetに対するフォールド
            max(
                case
                    when
                        action_name = 'FOLD'
                        and prev_action_name in ('BET', 'RAISE')
                        and exists (
                            select 1
                            from {{ ref("stg_player_actions") }} pa2
                            where
                                pa2.hand_id = pa1.hand_id
                                and pa2.player_id = prev_player_id
                                and pa2.phase = 0
                                and pa2.action_name in ('BET', 'RAISE')
                        )
                    then 1
                    else 0
                end
            ) as fold_to_cbet,
            -- アグレッション指標
            sum(
                case when action_name in ('BET', 'RAISE', 'ALL_IN') then 1 else 0 end
            ) as aggressive_actions,
            sum(case when action_name = 'CALL' then 1 else 0 end) as passive_actions,
            -- スタック情報
            coalesce(min(player_chip), 0) as starting_stack,
            coalesce(max(player_chip), 0) as final_stack,
            coalesce(sum(bet_chip), 0) as total_invested,
            -- プレッシャー指標の平均
            coalesce(avg(bet_chip::float), 0) as pressure_index
        from player_actions_base pa1
        group by hand_id, player_id
    ),

    hand_results as (
        select
            hand_id,
            player_id,
            hole_cards,
            hole_cards_str,
            -- ハンド文字列（例：AKs, AA, 72o）
            case
                when
                    (hole_cards[0] between 0 and 51)
                    and (hole_cards[1] between 0 and 51)
                then
                    case
                        -- ポケットペア判定
                        when
                            floor(hole_cards[0]::number / 4)
                            = floor(hole_cards[1]::number / 4)
                        then
                            case
                                when floor(hole_cards[0]::number / 4) = 12
                                then 'AA'
                                when floor(hole_cards[0]::number / 4) = 11
                                then 'KK'
                                when floor(hole_cards[0]::number / 4) = 10
                                then 'QQ'
                                when floor(hole_cards[0]::number / 4) = 9
                                then 'JJ'
                                when floor(hole_cards[0]::number / 4) = 8
                                then 'TT'
                                else
                                    lpad(
                                        to_char(floor(hole_cards[0]::number / 4) + 2),
                                        2,
                                        to_char(floor(hole_cards[0]::number / 4) + 2)
                                    )
                            end
                        else
                            -- 通常の2枚組
                            case
                                when
                                    greatest(
                                        floor(hole_cards[0]::number / 4),
                                        floor(hole_cards[1]::number / 4)
                                    )
                                    = 12
                                then 'A'
                                when
                                    greatest(
                                        floor(hole_cards[0]::number / 4),
                                        floor(hole_cards[1]::number / 4)
                                    )
                                    = 11
                                then 'K'
                                when
                                    greatest(
                                        floor(hole_cards[0]::number / 4),
                                        floor(hole_cards[1]::number / 4)
                                    )
                                    = 10
                                then 'Q'
                                when
                                    greatest(
                                        floor(hole_cards[0]::number / 4),
                                        floor(hole_cards[1]::number / 4)
                                    )
                                    = 9
                                then 'J'
                                when
                                    greatest(
                                        floor(hole_cards[0]::number / 4),
                                        floor(hole_cards[1]::number / 4)
                                    )
                                    = 8
                                then 'T'
                                else
                                    to_char(
                                        greatest(
                                            floor(hole_cards[0]::number / 4),
                                            floor(hole_cards[1]::number / 4)
                                        )
                                        + 2
                                    )
                            end || case
                                when
                                    least(
                                        floor(hole_cards[0]::number / 4),
                                        floor(hole_cards[1]::number / 4)
                                    )
                                    = 12
                                then 'A'
                                when
                                    least(
                                        floor(hole_cards[0]::number / 4),
                                        floor(hole_cards[1]::number / 4)
                                    )
                                    = 11
                                then 'K'
                                when
                                    least(
                                        floor(hole_cards[0]::number / 4),
                                        floor(hole_cards[1]::number / 4)
                                    )
                                    = 10
                                then 'Q'
                                when
                                    least(
                                        floor(hole_cards[0]::number / 4),
                                        floor(hole_cards[1]::number / 4)
                                    )
                                    = 9
                                then 'J'
                                when
                                    least(
                                        floor(hole_cards[0]::number / 4),
                                        floor(hole_cards[1]::number / 4)
                                    )
                                    = 8
                                then 'T'
                                else
                                    to_char(
                                        least(
                                            floor(hole_cards[0]::number / 4),
                                            floor(hole_cards[1]::number / 4)
                                        )
                                        + 2
                                    )
                            end
                            || case
                                when
                                    hole_cards[0]::number % 4
                                    = hole_cards[1]::number % 4
                                then 's'
                                else 'o'
                            end
                    end
                else null
            end as hand_str,
            -- ハンドタイプの判定
            case
                when
                    (hole_cards[0] between 0 and 51)
                    and (hole_cards[1] between 0 and 51)
                then
                    case
                        when
                            floor(hole_cards[0]::number / 4)
                            = floor(hole_cards[1]::number / 4)
                        then 'POCKET_PAIR'
                        when
                            hole_cards[0]::number % 4 = hole_cards[1]::number % 4
                            and abs(
                                floor(hole_cards[0]::number / 4)
                                - floor(hole_cards[1]::number / 4)
                            )
                            = 1
                        then 'SUITED_CONNECTOR'
                        when hole_cards[0]::number % 4 = hole_cards[1]::number % 4
                        then 'SUITED'
                        when
                            abs(
                                floor(hole_cards[0]::number / 4)
                                - floor(hole_cards[1]::number / 4)
                            )
                            = 1
                        then 'CONNECTOR'
                        else 'OFFSUIT'
                    end
                else null
            end as hand_type,
            -- ハンドカテゴリーの判定
            case
                when
                    (hole_cards[0] between 0 and 51)
                    and (hole_cards[1] between 0 and 51)
                then
                    case
                        when
                            floor(hole_cards[0]::number / 4)
                            = floor(hole_cards[1]::number / 4)
                            and floor(hole_cards[0]::number / 4) >= 9
                        then 'PREMIUM_PAIR'
                        when
                            floor(hole_cards[0]::number / 4)
                            = floor(hole_cards[1]::number / 4)
                            and floor(hole_cards[0]::number / 4) >= 6
                        then 'MEDIUM_PAIR'
                        when
                            floor(hole_cards[0]::number / 4)
                            = floor(hole_cards[1]::number / 4)
                        then 'SMALL_PAIR'
                        when
                            floor(hole_cards[0]::number / 4) >= 9
                            and floor(hole_cards[1]::number / 4) >= 9
                        then 'PREMIUM_CARDS'
                        when
                            floor(hole_cards[0]::number / 4) >= 6
                            and floor(hole_cards[1]::number / 4) >= 6
                        then 'MEDIUM_CARDS'
                        else 'SMALL_CARDS'
                    end
                else null
            end as hand_category,
            hand_ranking,
            case when reward_chip is not null then 1 else 0 end as went_to_showdown,
            case when reward_chip > 0 then 1 else 0 end as won_at_showdown,
            coalesce(reward_chip, 0) as net_profit
        from {{ ref("stg_hand_results") }}
    ),

    position_info as (
        select
            h.hand_id,
            pa.player_id,
            pa.seat_index,
            case
                when pa.seat_index = h.button_seat
                then 'BTN'
                when pa.seat_index = h.sb_seat
                then 'SB'
                when pa.seat_index = h.bb_seat
                then 'BB'
                when (pa.seat_index + 6 - h.button_seat) % 6 = 3
                then 'UTG'
                when (pa.seat_index + 6 - h.button_seat) % 6 = 4
                then 'HJ'
                when (pa.seat_index + 6 - h.button_seat) % 6 = 5
                then 'CO'
            end as position,
            h.bb
        from {{ ref("stg_hands") }} h
        cross join
            (
                select distinct hand_id, player_id, seat_index
                from {{ ref("stg_player_actions") }}
            ) pa
        where h.hand_id = pa.hand_id
    )

select
    -- 基本情報
    hr.hand_id,
    hr.player_id,
    pi.seat_index,
    pi.position,
    -- ホールカード情報
    hr.hole_cards,
    hr.hole_cards_str,
    hr.hand_str,
    hr.hand_type,
    hr.hand_category,
    hr.hand_ranking,
    -- スタック情報
    coalesce(pa.starting_stack, 0) as starting_stack,
    coalesce(pa.final_stack, 0) as final_stack,
    coalesce(pa.total_invested, 0) as total_invested,
    -- Mレシオ（スタック / BB）
    coalesce(
        case
            when coalesce(pi.bb, 0) > 0 then pa.starting_stack::float / pi.bb else 0
        end,
        0
    ) as m_ratio,
    -- 実効スタック（自分と相手の小さい方のスタック）
    coalesce(pa.starting_stack, 0) as effective_stack,
    -- アクション指標
    pa.is_vpip,
    pa.is_pfr,
    pa.is_3bet,
    pa.cbet_flop,
    pa.fold_to_cbet,
    -- 結果情報
    hr.went_to_showdown,
    hr.won_at_showdown,
    hr.net_profit,
    -- アグレッション指標
    case
        when coalesce(pa.passive_actions, 0) > 0
        then pa.aggressive_actions::float / pa.passive_actions
        else coalesce(pa.aggressive_actions::float, 0)
    end as aggression_factor,
    pa.pressure_index
from hand_results hr
join position_info pi on hr.hand_id = pi.hand_id and hr.player_id = pi.player_id
join player_actions pa on hr.hand_id = pa.hand_id and hr.player_id = pa.player_id
