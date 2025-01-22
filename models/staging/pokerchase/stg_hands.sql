/*
このモデルは、ポーカーハンドの基本情報とコミュニティカードを処理し、
ハンドの全体像を把握するためのデータを生成します。

主な処理の流れ：
1. ハンド開始情報の抽出（deal_events, best_deal_events）
   - EVT_DEAL (303) イベントからゲーム設定を取得
   - ブラインド情報（SB, BB, Ante）
   - シート情報（Button, SB, BB positions）
   - プレイヤー情報（seat_user_ids）
   - ゲームタイプ（RING_GAME/TOURNAMENT）

2. ハンド終了情報の取得（hand_results）
   - EVT_HAND_RESULTS (306) イベントから終了時刻を取得
   - 最後のイベントを選択

3. コミュニティカードの処理
   a. フェーズごとのカード抽出（flop_cards, turn_cards, river_cards）
      - EVT_DEAL_ROUND (305) イベントからカード情報を取得
      - フェーズ（FLOP=1, TURN=2, RIVER=3）ごとに最新の情報を使用

   b. 結果のカード確認（result_cards）
      - EVT_HAND_RESULTS (306) イベントからカード情報を取得
      - プリフロップオールインの場合に使用

   c. 最適なカード情報の選択（best_cards）
      - 各フェーズで最も多くのカード情報を持つレコードを選択
      - 一貫性のためにsender_user_idも考慮

4. カード情報の統合（combined_cards, card_strings）
   - 全フェーズのカードを結合
   - プリフロップオールインの場合は結果のカードを使用
   - カード配列を文字列に変換

5. 最終的な出力
   - ハンドの基本情報（hand_id, timestamps, game_type）
   - コミュニティカード情報（配列と文字列形式）
   - テーブル設定（positions, blinds, ante）
   - プレイヤー配置（seat_user_ids）
*/

with deal_events as (
    select
        hand_id,
        sender_user_id,
        value:Game:ButtonSeat::number as button_seat,
        value:Game:SmallBlindSeat::number as sb_seat,
        value:Game:BigBlindSeat::number as bb_seat,
        value:Game:SmallBlind::number as sb,
        value:Game:BigBlind::number as bb,
        value:Game:Ante::number as ante,
        value:Game:CurrentBlindLv::number as current_blind_lv,
        CASE
            WHEN value:Game:NextBlindUnixSeconds::number = -1 THEN 'RING_GAME'
            ELSE 'TOURNAMENT'
        END as game_type,
        value:Game:NextBlindUnixSeconds::number as next_blind_unix_seconds,
        value:SeatUserIds as seat_user_ids,
        event_timestamp as start_timestamp
    from {{ ref('stg_hand_events') }}
    where ApiTypeId = 303  -- EVT_DEAL
    and is_hand_start = 1  -- ハンドの開始時のみ
),

-- best_deal_events CTE
-- 目的：同一hand_idで最も多くの情報を持つレコードを選択します
-- 処理内容：
-- - seat_user_idsの要素数（プレイヤー情報の量）を優先
-- - タイムスタンプが早いものを優先
best_deal_events as (
    select *
    from deal_events
    qualify row_number() over (
        partition by hand_id
        order by
            -- 優先順位:
            -- 1. seat_user_idsの要素数が多い（より多くのプレイヤー情報を持つ）
            array_size(seat_user_ids) desc,
            -- 2. より早いタイムスタンプ
            start_timestamp asc
    ) = 1
),

-- hand_results CTE
-- 目的：ハンドの終了情報を取得します
-- 処理内容：
-- - EVT_HAND_RESULTS (306) から最後のイベントを選択
-- - 終了時刻を記録
hand_results as (
    select
        hand_id,
        sender_user_id,
        event_timestamp as end_timestamp
    from {{ ref('stg_hand_events') }}
    where ApiTypeId = 306  -- EVT_HAND_RESULTS
    qualify row_number() over (
        partition by hand_id
        order by event_timestamp desc
    ) = 1
),

-- flop_cards CTE
-- 目的：フロップのカード情報を取得します
-- 処理内容：
-- - EVT_DEAL_ROUND (305) のフェーズ1（FLOP）からカード情報を抽出
-- - 最新のイベントを選択
flop_cards as (
    select
        hand_id,
        sender_user_id,
        value:CommunityCards as cards,
        value:Progress:Phase::number as phase
    from {{ ref('stg_hand_events') }}
    where ApiTypeId = 305  -- EVT_DEAL_ROUND
    and value:Progress:Phase::number = 1
    and value:CommunityCards is not null
    qualify row_number() over (
        partition by hand_id
        order by event_timestamp desc
    ) = 1
),

-- turn_cards CTE
-- 目的：ターンのカード情報を取得します
-- 処理内容：
-- - EVT_DEAL_ROUND (305) のフェーズ2（TURN）からカード情報を抽出
-- - 最新のイベントを選択
turn_cards as (
    select
        hand_id,
        sender_user_id,
        value:CommunityCards as cards,
        value:Progress:Phase::number as phase
    from {{ ref('stg_hand_events') }}
    where ApiTypeId = 305  -- EVT_DEAL_ROUND
    and value:Progress:Phase::number = 2
    and value:CommunityCards is not null
    qualify row_number() over (
        partition by hand_id
        order by event_timestamp desc
    ) = 1
),

-- river_cards CTE
-- 目的：リバーのカード情報を取得します
-- 処理内容：
-- - EVT_DEAL_ROUND (305) のフェーズ3（RIVER）からカード情報を抽出
-- - 最新のイベントを選択
river_cards as (
    select
        hand_id,
        sender_user_id,
        value:CommunityCards as cards,
        value:Progress:Phase::number as phase
    from {{ ref('stg_hand_events') }}
    where ApiTypeId = 305  -- EVT_DEAL_ROUND
    and value:Progress:Phase::number = 3
    and value:CommunityCards is not null
    qualify row_number() over (
        partition by hand_id
        order by event_timestamp desc
    ) = 1
),

-- result_cards CTE
-- 目的：ハンド結果のカード情報を取得します（プリフロップオールインの場合に使用）
-- 処理内容：
-- - EVT_HAND_RESULTS (306) からカード情報を抽出
-- - 空配列を除外
-- - 最新のイベントを選択
result_cards as (
    select
        hand_id,
        sender_user_id,
        value:CommunityCards as cards
    from {{ ref('stg_hand_events') }}
    where ApiTypeId = 306  -- EVT_HAND_RESULTS
    and value:CommunityCards is not null
    and array_size(value:CommunityCards) > 0  -- 空配列を除外
    qualify row_number() over (
        partition by hand_id
        order by event_timestamp desc
    ) = 1
),

-- best_cards CTE
-- 目的：各フェーズで最も信頼できるカード情報を選択します
-- 処理内容：
-- - 全フェーズのカード情報を統合（UNION ALL）
-- - カードの要素数とsender_user_idを基準に最適なレコードを選択
best_cards as (
    select
        hand_id,
        cards,
        sender_user_id,
        'FLOP' as phase
    from flop_cards
    union all
    select
        hand_id,
        cards,
        sender_user_id,
        'TURN' as phase
    from turn_cards
    union all
    select
        hand_id,
        cards,
        sender_user_id,
        'RIVER' as phase
    from river_cards
    union all
    select
        hand_id,
        cards,
        sender_user_id,
        'RESULT' as phase
    from result_cards
    qualify row_number() over (
        partition by hand_id, phase
        order by
            -- 優先順位:
            -- 1. カードの要素数が多い
            array_size(cards) desc nulls last,
            -- 2. sender_user_idの値が小さい（一貫性のため）
            sender_user_id asc
    ) = 1
),

-- combined_cards CTE
-- 目的：全フェーズのカード情報を1つの配列に統合します
-- 処理内容：
-- - 通常のフェーズ進行の場合は各フェーズのカードを結合
-- - プリフロップオールインの場合は結果のカードを使用
-- - 空の場合は空配列を返す
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
    from best_deal_events d
    left join best_cards f
        on d.hand_id = f.hand_id
        and f.phase = 'FLOP'
    left join best_cards t
        on d.hand_id = t.hand_id
        and t.phase = 'TURN'
    left join best_cards r
        on d.hand_id = r.hand_id
        and r.phase = 'RIVER'
    left join best_cards res
        on d.hand_id = res.hand_id
        and res.phase = 'RESULT'
),

-- card_strings CTE
-- 目的：カード配列を文字列形式に変換します
-- 処理内容：
-- - convert_card_array_to_strings マクロを使用してカードを文字列に変換
-- - hand_idごとにグループ化
card_strings as (
    select
        c.hand_id,
        c.community_cards,
        {{ convert_card_array_to_strings('value') }} as community_cards_str
    from combined_cards c
    cross join table(flatten(input => c.community_cards)) as cards
    group by c.hand_id, c.community_cards
)

-- 最終的な出力
-- - ハンドの基本情報（hand_id, timestamps, game_type）
-- - コミュニティカード情報（配列と文字列形式）
-- - テーブル設定（positions, blinds, ante）
-- - プレイヤー配置（seat_user_ids）
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
    d.next_blind_unix_seconds
from best_deal_events d
left join card_strings c
    on d.hand_id = c.hand_id
left join hand_results hr
    on d.hand_id = hr.hand_id
where d.hand_id is not null
