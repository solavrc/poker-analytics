with
    source as (select * from {{ ref("stg_player_actions") }}),

    action_metrics as (
        select
            hand_id,
            player_id,
            phase,
            case phase when 0 then 'PREFLOP' when 1 then 'FLOP' when 2 then 'TURN' when 3 then 'RIVER' end as phase_name,
            event_timestamp,
            action_order,
            action_name,
            bet_chip,
            pot,
            player_chip,
            bb,
            -- スタックとポットの比率
            case when pot = 0 then null else player_chip::float / pot end as stack_to_pot_ratio,
            -- プレッシャー指標
            case when bb = 0 then null else bet_chip::float / bb end as pressure_index,
            -- アクション種別ごとのカウント
            case when action_name in ('BET', 'RAISE', 'ALL_IN') then 1 else 0 end as aggressive_action,
            case when action_name = 'CALL' then 1 else 0 end as passive_action,
            case when action_name = 'CHECK' then 1 else 0 end as check_action,
            case when action_name = 'FOLD' then 1 else 0 end as fold_action
        from source
    ),

    action_diversity as (
        select
            hand_id,
            player_id,
            phase,
            phase_name,
            event_timestamp,
            action_order,
            action_name,
            bet_chip,
            pot,
            player_chip,
            bb,
            stack_to_pot_ratio,
            pressure_index,
            aggressive_action,
            passive_action,
            check_action,
            fold_action,
            -- アクションの多様性（0-1の値）
            case
                when (aggressive_action + passive_action + check_action + fold_action) = 0
                then 0
                else
                    (
                        case when aggressive_action > 0 then 1 else 0 end
                        + case when passive_action > 0 then 1 else 0 end
                        + case when check_action > 0 then 1 else 0 end
                        + case when fold_action > 0 then 1 else 0 end
                    )::float
                    / 4
            end as action_diversity
        from action_metrics
    ),

    player_action_flags as (
        select
            hand_id,
            player_id,
            phase_name,
            -- プリフロップアクション指標
            max(case when phase_name = 'PREFLOP' and action_name in ('CALL', 'BET', 'RAISE', 'ALL_IN') then 1 else 0 end) as is_vpip,
            max(case when phase_name = 'PREFLOP' and action_name in ('BET', 'RAISE', 'ALL_IN') then 1 else 0 end) as is_pfr,
            max(case when phase_name = 'PREFLOP' and action_name in ('RAISE', 'ALL_IN') and action_order > 1 then 1 else 0 end) as is_3bet,
            -- フロップアクション指標（改善版CBET）
            max(
                case
                    when
                        phase_name = 'FLOP'
                        and action_name in ('BET', 'RAISE', 'ALL_IN')
                        and exists (
                            select 1
                            from action_diversity pa_pre
                            where
                                pa_pre.hand_id = action_diversity.hand_id
                                and pa_pre.player_id = action_diversity.player_id
                                and pa_pre.phase_name = 'PREFLOP'
                                and pa_pre.action_name in ('BET', 'RAISE', 'ALL_IN')
                                and not exists (
                                    select 1
                                    from action_diversity pa_later
                                    where
                                        pa_later.hand_id = pa_pre.hand_id
                                        and pa_later.player_id = pa_pre.player_id
                                        and pa_later.phase_name = 'PREFLOP'
                                        and pa_later.action_order > pa_pre.action_order
                                        and pa_later.action_name not in ('BET', 'RAISE', 'ALL_IN')
                                )
                        )
                    then 1
                    else 0
                end
            ) as cbet_flop,
            -- アグレッション指標
            sum(aggressive_action) as aggressive_actions,
            sum(passive_action) as passive_actions
        from action_diversity
        group by hand_id, player_id, phase_name
    )

select hand_id, player_id, phase_name, is_vpip, is_pfr, is_3bet, aggressive_actions, passive_actions, cbet_flop
from player_action_flags
