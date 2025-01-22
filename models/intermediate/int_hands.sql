with
    hand_results as (
        select
            hand_id,
            player_id,
            hole_cards_str,
            -- ハンドタイプの判定
            case
                when hole_cards_str[0] is not null and hole_cards_str[1] is not null
                then
                    case
                        when floor(hole_cards_str[0] / 13) = floor(hole_cards_str[1] / 13)
                        then 'POCKET_PAIR'
                        when mod(hole_cards_str[0], 13) = mod(hole_cards_str[1], 13) and abs(floor(hole_cards_str[0] / 13) - floor(hole_cards_str[1] / 13)) = 1
                        then 'SUITED_CONNECTOR'
                        when mod(hole_cards_str[0], 13) = mod(hole_cards_str[1], 13)
                        then 'SUITED'
                        when abs(floor(hole_cards_str[0] / 13) - floor(hole_cards_str[1] / 13)) = 1
                        then 'CONNECTOR'
                        else 'OFFSUIT'
                    end
                else null
            end as hand_type,
            -- ハンドカテゴリーの判定
            case
                when hole_cards_str[0] is not null and hole_cards_str[1] is not null
                then
                    case
                        when floor(hole_cards_str[0] / 13) = floor(hole_cards_str[1] / 13) and floor(hole_cards_str[0] / 13) >= 10
                        then 'PREMIUM_PAIR'
                        when floor(hole_cards_str[0] / 13) = floor(hole_cards_str[1] / 13) and floor(hole_cards_str[0] / 13) >= 7
                        then 'MEDIUM_PAIR'
                        when floor(hole_cards_str[0] / 13) = floor(hole_cards_str[1] / 13)
                        then 'SMALL_PAIR'
                        when floor(hole_cards_str[0] / 13) >= 10 and floor(hole_cards_str[1] / 13) >= 10
                        then 'PREMIUM_CARDS'
                        when floor(hole_cards_str[0] / 13) >= 7 and floor(hole_cards_str[1] / 13) >= 7
                        then 'MEDIUM_CARDS'
                        else 'SMALL_CARDS'
                    end
                else null
            end as hand_category,
            hand_ranking as hand_strength,
            case when reward_chip > 0 then 1 else 0 end as won_at_showdown,
            case when reward_chip is not null then 1 else 0 end as went_to_showdown,
            coalesce(reward_chip, 0) as net_profit
        from {{ ref("stg_hand_results") }}
    ),

    showdown_info as (
        select
            hand_id,
            count(distinct player_id) as players_at_showdown,
            sum(case when won_at_showdown = 1 then 1 else 0 end) as winners_at_showdown,
            sum(case when net_profit > 0 then 1 else 0 end) as winners_count,
            sum(net_profit) as total_profit,
            max(hand_strength) as winning_hand_strength
        from hand_results
        group by hand_id
    ),

    pot_info as (
        select
            pa.hand_id,
            -- アクティブプレイヤー数の修正：実際にアクションに参加したプレイヤー数
            greatest(
                -- 通常のアクティブプレイヤー計算
                count(
                    distinct
                    case
                        when pa.action_name = 'FOLD'
                        then null  -- FOLDしたプレイヤーは除外
                        when pa.action_name in ('BET', 'RAISE', 'CALL', 'ALL_IN')
                        then pa.player_id  -- 積極的なアクション
                        when
                            pa.action_name = 'CHECK'
                            and (
                                pa.phase > 0  -- プリフロップ以外でのCHECK
                                or (pa.phase = 0 and h.bb_seat = pa.seat_index)  -- BBポジションでのCHECK
                            )
                        then pa.player_id
                        else null
                    end
                ),
                -- オールインケースの最小プレイヤー数（2）を保証
                case
                    when exists (select 1 from {{ ref("stg_player_actions") }} pa_all_in where pa_all_in.hand_id = pa.hand_id and pa_all_in.action_name = 'ALL_IN')
                    then 2
                    else 0
                end,
                -- コールケースの最小プレイヤー数（2）を保証
                case when exists (select 1 from {{ ref("stg_player_actions") }} pa_call where pa_call.hand_id = pa.hand_id and pa_call.action_name = 'CALL') then 2 else 0 end,
                -- ポット獲得者が存在する場合は最低1人を保証
                case when exists (select 1 from {{ ref("stg_hand_results") }} hr where hr.hand_id = pa.hand_id and hr.reward_chip > 0) then 1 else 0 end,
                -- ショーダウンプレイヤー数を保証
                coalesce((select count(distinct player_id) from {{ ref("stg_hand_results") }} hr where hr.hand_id = pa.hand_id and hr.reward_chip is not null), 0)
            ) as active_players,
            count(distinct case when pa.phase = 0 and pa.action_name in ('BET', 'RAISE', 'CALL') then pa.player_id end) as vpip_players,
            count(distinct case when pa.phase = 0 and pa.action_name in ('BET', 'RAISE') then pa.player_id end) as pfr_players
        from {{ ref("stg_player_actions") }} pa
        left join {{ ref("stg_hands") }} h on pa.hand_id = h.hand_id
        group by pa.hand_id
    ),

    final_pot_info as (select hand_id, sum(reward_chip) as final_pot from {{ ref("stg_hand_results") }} group by hand_id),

    hand_phases as (
        select
            hand_id, max(phase) as last_phase, count(distinct phase) as total_phases, count(distinct case when action_name in ('BET', 'RAISE') then phase end) as betting_phases
        from {{ ref("stg_player_actions") }}
        group by hand_id
    ),

    hand_info as (
        select h.hand_id, h.start_timestamp, h.game_type, h.community_cards, h.community_cards_str, h.button_seat, h.sb_seat, h.bb_seat, h.bb, from {{ ref("stg_hands") }} h
    )

select
    -- 基本情報
    hi.hand_id,
    hi.start_timestamp,
    hi.game_type,
    hi.button_seat,
    hi.sb_seat,
    hi.bb_seat,
    hi.bb,
    -- コミュニティカード情報
    hi.community_cards,
    hi.community_cards_str,
    -- ポット情報
    fpi.final_pot,
    -- プレイヤー情報
    pi.active_players,
    pi.vpip_players,
    pi.pfr_players,
    si.players_at_showdown,
    si.winners_at_showdown,
    si.winners_count,
    -- フェーズ情報
    hp.last_phase,
    hp.total_phases,
    hp.betting_phases,
    -- 結果情報
    si.total_profit,
    si.winning_hand_strength
from hand_info hi
left join pot_info pi on hi.hand_id = pi.hand_id
left join showdown_info si on hi.hand_id = si.hand_id
left join hand_phases hp on hi.hand_id = hp.hand_id
left join final_pot_info fpi on hi.hand_id = fpi.hand_id
