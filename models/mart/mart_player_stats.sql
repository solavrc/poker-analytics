/*
このモデルは、プレイヤーごとのポーカープレイスタイルと成績を分析し、
様々な指標を計算して総合的な評価を行います。

主な処理の流れ：
1. プレイヤーアクションの抽出（player_actions）
   - int_player_actionsから必要な情報を取得
   - トーナメントのハンドのみを対象

2. ショーダウン結果の処理（showdown_results）
   - ショーダウンの参加有無
   - 勝敗結果
   - 収支情報（net_profit, total_invested）を取得
   - ホールカードの有無でプレイヤーの状態を判定

3. プレイヤー統計の計算（player_stats）
   a. プリフロップ指標
      - VPIP（Voluntarily Put money In Pot）
      - PFR（PreFlop Raise）
      - 3BET

   b. ポストフロップ指標
      - フロップCベット
      - アグレッション（アグレッシブアクション vs パッシブアクション）
      - ストリート到達率と勝率

   c. ショーダウン統計
      - ショーダウン到達率
      - ショーダウン勝率
      - フロップ以降の勝率

   d. 収益性指標
      - 総利益
      - 総投資額
      - 平均ハンド収益
      - ROI

4. 最終的な出力
   - 基本統計（total_hands）
   - プリフロップ指標（vpip_ratio, pfr_ratio, threeBet_ratio）
   - ポストフロップ指標（flopCB_ratio, aggression_factor, aggression_frequency）
   - ストリート統計（flop_seen_ratio, turn_seen_ratio, river_seen_ratio）
   - 勝率指標（won_after_flop_ratio, won_after_turn_ratio, won_after_river_ratio）
   - 収益指標（total_profit, total_invested, avg_profit_per_hand, roi）
*/
with
    player_actions as (select pa.* from {{ ref("int_player_actions") }} pa join {{ ref("stg_hands") }} h on pa.hand_id = h.hand_id),

    -- showdown_results CTE
    -- 目的：ショーダウンの結果とプレイヤーの状態を把握します
    -- 処理内容：
    -- - ショーダウンの参加有無と勝敗を記録
    -- - 収支情報（net_profit, total_invested）を取得
    -- - ホールカードの有無でプレイヤーの状態を判定
    showdown_results as (
        select
            sr.player_id,
            sr.hand_id,
            sr.went_to_showdown,
            sr.won_at_showdown,
            coalesce(sr.net_profit, 0) as net_profit,  -- NULLを0に変換
            coalesce(sr.total_invested, 0) as total_invested,  -- NULLを0に変換
            -- ホールカードの状態判定を追加
            case
                when array_size(sr.hole_cards) = 0
                then 0  -- フォールドしたプレイヤー
                else 1  -- アクティブなプレイヤー
            end as has_hole_cards
        from {{ ref("int_player_hands") }} sr
        inner join {{ ref("stg_hands") }} h on sr.hand_id = h.hand_id
        where h.game_type = 'TOURNAMENT'
    ),

    -- player_stats CTE
    -- 目的：プレイヤーごとの詳細な統計を計算します
    -- 処理内容：
    -- - 基本統計（total_hands）
    -- - プリフロップ指標（VPIP, PFR, 3BET）
    -- - ポストフロップ指標（フロップCB, アグレッション）
    -- - ストリート統計（到達率、勝率）
    -- - 収益性指標（利益、投資額、平均収益）
    player_stats as (
        select
            pa.player_id,
            -- 基本統計
            count(distinct pa.hand_id) as total_hands,
            -- VPIP
            count(distinct case when pa.is_vpip = 1 then pa.hand_id end) as vpip_hands,
            -- PFR
            count(distinct case when pa.is_pfr = 1 then pa.hand_id end) as pfr_hands,
            -- 3BET
            count(distinct case when pa.is_3bet = 1 then pa.hand_id end) as threebet_hands,
            -- FLOP CB
            count(distinct case when pa.cbet_flop = 1 then pa.hand_id end) as flopcb_hands,
            -- アグレッション（アクティブなプレイヤーのみ）
            sum(case when sr.has_hole_cards = 1 then pa.aggressive_actions else 0 end) as aggressive_actions,
            sum(case when sr.has_hole_cards = 1 then pa.passive_actions else 0 end) as passive_actions,
            sum(case when sr.has_hole_cards = 1 then pa.aggressive_actions + pa.passive_actions else 0 end) as total_noncheck_actions,
            -- ショーダウン統計（アクティブなプレイヤーのみ）
            count(distinct case when sr.has_hole_cards = 1 and sr.went_to_showdown = 1 then sr.hand_id end) as showdown_hands,
            count(distinct case when sr.has_hole_cards = 1 and sr.won_at_showdown = 1 then sr.hand_id end) as won_at_showdown_hands,
            -- フロップ到達（アクティブなプレイヤーのみ）
            count(distinct case when sr.has_hole_cards = 1 and pa.phase_name = 'FLOP' then pa.hand_id end) as saw_flop_hands,
            count(distinct case when sr.has_hole_cards = 1 and pa.phase_name = 'FLOP' and sr.won_at_showdown = 1 then pa.hand_id end) as won_after_flop_hands,
            -- ターン到達（アクティブなプレイヤーのみ）
            count(distinct case when sr.has_hole_cards = 1 and pa.phase_name = 'TURN' then pa.hand_id end) as saw_turn_hands,
            count(distinct case when sr.has_hole_cards = 1 and pa.phase_name = 'TURN' and sr.won_at_showdown = 1 then pa.hand_id end) as won_after_turn_hands,
            -- リバー到達（アクティブなプレイヤーのみ）
            count(distinct case when sr.has_hole_cards = 1 and pa.phase_name = 'RIVER' then pa.hand_id end) as saw_river_hands,
            count(distinct case when sr.has_hole_cards = 1 and pa.phase_name = 'RIVER' and sr.won_at_showdown = 1 then pa.hand_id end) as won_after_river_hands,
            -- 収益性指標
            sum(sr.net_profit) as total_profit,
            sum(sr.total_invested) as total_invested,
            avg(sr.net_profit) as avg_profit_per_hand
        from player_actions pa
        left join showdown_results sr on pa.player_id = sr.player_id and pa.hand_id = sr.hand_id
        group by pa.player_id
    )

-- 最終的な出力
-- - 基本統計（total_hands）
-- - プリフロップ指標（vpip_ratio, pfr_ratio, threeBet_ratio）
-- - ポストフロップ指標（flopCB_ratio, aggression_factor, aggression_frequency）
-- - ストリート統計（flop_seen_ratio, turn_seen_ratio, river_seen_ratio）
-- - 勝率指標（won_after_flop_ratio, won_after_turn_ratio, won_after_river_ratio）
-- - 収益指標（total_profit, total_invested, avg_profit_per_hand, roi）
select
    player_id,
    total_hands,
    -- VPIP
    vpip_hands,
    coalesce(least(1, round(vpip_hands::float / nullif(total_hands, 0), 4)), 0) as vpip_ratio,
    -- PFR
    pfr_hands,
    coalesce(least(1, round(pfr_hands::float / nullif(total_hands, 0), 4)), 0) as pfr_ratio,
    -- 3BET
    threebet_hands,
    coalesce(least(1, round(threebet_hands::float / nullif(total_hands, 0), 4)), 0) as threebet_ratio,
    -- FLOP CB
    flopcb_hands,
    coalesce(least(1, round(flopcb_hands::float / nullif(saw_flop_hands, 0), 4)), 0) as flopcb_ratio,
    -- アグレッション
    coalesce(round(aggressive_actions::float / nullif(passive_actions, 0), 4), 0) as aggression_factor,
    coalesce(least(1, round(aggressive_actions::float / nullif(total_noncheck_actions, 0), 4)), 0) as aggression_frequency,
    -- ショーダウン統計
    coalesce(least(1, round(showdown_hands::float / nullif(saw_flop_hands, 0), 4)), 0) as went_to_showdown_ratio,
    coalesce(least(1, round(won_after_flop_hands::float / nullif(saw_flop_hands, 0), 4)), 0) as won_when_saw_flop_ratio,
    coalesce(least(1, round(won_at_showdown_hands::float / nullif(showdown_hands, 0), 4)), 0) as won_at_showdown_ratio,
    -- ストリート到達率
    coalesce(least(1, round(saw_flop_hands::float / nullif(total_hands, 0), 4)), 0) as flop_seen_ratio,
    coalesce(least(1, round(saw_turn_hands::float / nullif(saw_flop_hands, 0), 4)), 0) as turn_seen_ratio,
    coalesce(least(1, round(saw_river_hands::float / nullif(saw_turn_hands, 0), 4)), 0) as river_seen_ratio,
    -- ストリート別勝率
    coalesce(least(1, round(won_after_flop_hands::float / nullif(saw_flop_hands, 0), 4)), 0) as won_after_flop_ratio,
    coalesce(least(1, round(won_after_turn_hands::float / nullif(saw_turn_hands, 0), 4)), 0) as won_after_turn_ratio,
    coalesce(least(1, round(won_after_river_hands::float / nullif(saw_river_hands, 0), 4)), 0) as won_after_river_ratio,
    -- 収益性指標
    coalesce(total_profit, 0) as total_profit,
    coalesce(total_invested, 0) as total_invested,
    coalesce(avg_profit_per_hand, 0) as avg_profit_per_hand,
    case when coalesce(total_invested, 0) = 0 then 0 else round(coalesce(total_profit, 0)::float / total_invested, 4) end as roi
from player_stats
where total_hands >= 10  -- 最低10ハンド以上プレイしたプレイヤーのみを対象
