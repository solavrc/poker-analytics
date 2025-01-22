/*
このモデルは、ポーカーハンドの各フェーズ（PREFLOP, FLOP, TURN, RIVER）における
アクション統計とポット推移を集計します。

主な処理の流れ：
1. フェーズイベントの抽出（phase_events）
   - ハンドイベントからフェーズ情報とポット情報を取得
   - フェーズ名のマッピング（0=PREFLOP, 1=FLOP, 2=TURN, 3=RIVER）
   - フェーズの開始と終了を示すフラグを設定

2. フェーズの境界を特定（phase_boundaries）
   - 各フェーズの開始時と終了時のポットを記録
   - フェーズごとのポット推移を把握

3. フェーズごとのアクション集計（phase_actions）
   - アクションの総数
   - フォールド回数
   - アグレッシブなアクション（BET, RAISE, ALL_IN）の回数
   - アクティブプレイヤー数
   - 合計ベット額

4. 最終的な出力
   - フェーズ情報（phase_name）
   - ポット推移（starting_pot, ending_pot）
   - アクション統計（total_actions, fold_actions, aggressive_actions）
   - プレイヤー情報（active_players）
   - ベット情報（total_bets）
   - アグレッション指標（aggression_frequency）
*/
with
    phase_events as (
        select
            hand_id,
            event_timestamp,
            phase,
            pot,
            -- フェーズ名のマッピング
            case phase when 0 then 'PREFLOP' when 1 then 'FLOP' when 2 then 'TURN' when 3 then 'RIVER' end as phase_name,
            -- フェーズの開始を示すフラグ
            case
                when phase != lag(phase) over (partition by hand_id order by event_timestamp) or lag(phase) over (partition by hand_id order by event_timestamp) is null
                then 1
                else 0
            end as is_phase_start,
            -- フェーズの終了を示すフラグ
            case
                when phase != lead(phase) over (partition by hand_id order by event_timestamp) or lead(phase) over (partition by hand_id order by event_timestamp) is null
                then 1
                else 0
            end as is_phase_end
        from {{ ref("stg_hand_events") }}
        where hand_id is not null and phase is not null  -- フェーズが不明なイベントを除外
    ),

    -- phase_boundaries CTE
    -- 目的：各フェーズの開始時と終了時のポットを記録します
    -- 処理内容：
    -- - フェーズごとに最小ポット（開始時）と最大ポット（終了時）を計算
    -- - hand_id, phase_name, phaseでグループ化して集計
    phase_boundaries as (
        select
            hand_id,
            phase_name,
            phase,
            min(pot) as starting_pot,  -- フェーズ開始時のポット
            max(pot) as ending_pot  -- フェーズ終了時のポット
        from phase_events
        where phase_name is not null  -- フェーズ名が不明なイベントを除外
        group by hand_id, phase_name, phase
    ),

    -- phase_actions CTE
    -- 目的：フェーズごとのアクション統計を集計します
    -- 処理内容：
    -- - アクションの総数をカウント
    -- - フォールド、アグレッシブアクション（BET, RAISE, ALL_IN）の回数を集計
    -- - アクティブプレイヤー数（ユニークなplayer_id）をカウント
    -- - 合計ベット額を計算
    phase_actions as (
        select
            hand_id,
            phase,
            count(*) as total_actions,
            sum(case when action_name in ('FOLD') then 1 else 0 end) as fold_actions,
            sum(case when action_name in ('BET', 'RAISE', 'ALL_IN') then 1 else 0 end) as aggressive_actions,
            count(distinct player_id) as active_players,
            sum(bet_chip) as total_bets
        from {{ ref("stg_player_actions") }}
        where hand_id is not null and phase is not null  -- フェーズが不明なイベントを除外
        group by hand_id, phase
    )

-- 最終的な出力
-- - フェーズ情報（phase_name）とポット推移（starting_pot, ending_pot）
-- - アクション統計（total_actions, fold_actions, aggressive_actions）
-- - プレイヤー情報（active_players）とベット情報（total_bets）
-- - アグレッション指標（aggression_frequency）を計算
select
    pb.hand_id,
    pb.phase_name,
    pb.starting_pot,
    pb.ending_pot,
    coalesce(pa.total_actions, 0) as total_actions,
    coalesce(pa.fold_actions, 0) as fold_actions,
    coalesce(pa.aggressive_actions, 0) as aggressive_actions,
    coalesce(pa.active_players, 0) as active_players,
    coalesce(pa.total_bets, 0) as total_bets,
    -- アグレッション頻度（攻撃的なアクションの割合）
    case when coalesce(pa.total_actions, 0) > 0 then cast(pa.aggressive_actions as float) / pa.total_actions else 0 end as aggression_frequency
from phase_boundaries pb
left join phase_actions pa on pb.hand_id = pa.hand_id and pb.phase = pa.phase
order by pb.hand_id, pb.phase
