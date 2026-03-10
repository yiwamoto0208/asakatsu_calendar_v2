import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
import calendar
import pandas as pd
import uuid

# --- ページ設定 ---
st.set_page_config(
    page_title="見守りシフト管理カレンダー v2",
    page_icon="🗓️",
    layout="wide"
)

# --- 定数定義 ---
# Firestoreのルールに従い、パスを要素1つのシンプルなものに変更
# 日本時間のタイムゾーンを設定（UTC+9時間）
JST = timezone(timedelta(hours=+9), 'JST')

EVENTS_COLLECTION = "v2_events"
DAY_STATUS_COLLECTION = "v2_day_status"
MONTH_LOCKS_COLLECTION = "v2_month_locks"
BOARD_COLLECTION = "v2_bulletin_board"
MAX_SHIFTS_PER_DAY = 5  # 1日に許可される最大シフト数

# ユーザー属性と文字色の定義
USER_ATTRIBUTES = {
    "PTA": "blue",
    "地域ボランティア（社会人）": "green",
    "地域ボランティア（学生）": "darkorange",
    "その他": "gray"
}
MINUTES_PER_SHIFT = 50  # 1回あたりの活動時間(分)

# --- マニュアルのテキスト定義 ---
USER_MANUAL_TEXT = """# 🗓️ 見守りシフト管理カレンダー 使い方ガイド

見守り活動にご参加いただき、ありがとうございます！
このカレンダーアプリを使えば、スマートフォンやパソコンからいつでも簡単にシフトの登録や確認ができます。

## ⚠️ 1. アプリを開くときの重要なお願い（スリープ画面について）
アクセスした際、カレンダーではなく以下のような英語の画面が表示されることがあります。
> "Yes, get this app back up!" （はい、このアプリを再起動します！）
これはエラーではなく、アプリがお休み状態（スリープ）に入っているだけです。
この画面が出たら、青いボタンをクリック（タップ）して、数十秒〜1分ほどお待ちください。 カレンダーが自動的に表示されます。

## 2. はじめの準備（毎回）
1. 管理者から共有されたURLにアクセスします。
2. セキュリティに関する注意事項が表示されます。URLは関係者以外に教えないでください。 確認後、「同意してアプリを利用する」を押します。
3. お名前（フルネーム） と ご自身の属性（PTA、地域ボランティアなど） を入力し、「利用開始」を押します。
   ※入力したお名前や状態は、ブラウザを閉じるまで保持されます。ブラウザを閉じた場合は再度ご入力をお願いします。

## 3. シフトの登録と取り消し
カレンダーが表示されたら、自分の入りたい日を選びます。
* シフトに入る
  「開催日」となっている日付の枠内にある [シフトに入る] ボタンを押すだけで登録完了です！あなたのお名前がカレンダーに表示されます。
  ※1日最大5名までの先着順です。「🈵 満員です」と表示されている日は登録できません。
* シフトを取り消す
  間違えて登録してしまった場合や、予定が変わった場合は、ご自身のお名前の横にある [✖️] ボタンを押すと取り消しができます。
  ※すでに月が「ロック」されて確定している場合は、ご自身での取り消しはできません。管理者に直接ご連絡ください。

## 📊 4. 活動実績の確認について
月末になり、管理者がその月のカレンダーを「ロック（確定）」すると、カレンダーの下部にその月の活動実績（参加者ごとの合計活動時間）が表示されるようになります。
ご自身の活動時間を確認したい場合は、カレンダー上部の「<< 前の月」ボタンを押して、過去の月を表示してご確認ください。

## 📢 5. 緊急連絡掲示板について
画面の下部には、誰でも自由に書き込める「緊急連絡掲示板」 があります。
「今日は雨で中止にします」「少し遅れます」などの連絡に活用してください。（※投稿から2週間経つと自動で消去されます）

🚨 【超重要】赤い点滅アラートについて 🚨
過去24時間以内に掲示板に新しい連絡が書き込まれると、画面の一番上に、赤く点滅する目立つお知らせ が表示されます。
この赤い表示が出ている時は、大切な連絡が来ているサインです。必ず画面下の掲示板までスクロールして確認してください！
"""

ADMIN_MANUAL_TEXT = """# 🛠️ 見守りシフト管理カレンダー 管理者マニュアル

このマニュアルは、システムの管理・運用を行う方向けのガイドです。
一般ユーザーには見えない「管理者メニュー」の操作方法を解説します。

## 1. 管理者メニューへのログイン
1. カレンダーアプリを開きます。
2. 画面左上にある [ > ] のようなマーク（サイドバー展開ボタン）をクリックします。
3. 「🛠️ 管理者メニュー」が開きます。
4. あらかじめ設定された 管理者パスワード を入力し、「ログイン」を押します。

## 2. 日々の運用管理（カレンダーの直接編集）
管理者としてログインすると、カレンダー上に特別な操作ボタンが出現します。
* 開催日の設定（ON/OFF）
  各日付の左上にある「開催」のチェックボックスを操作できます。チェックを外すと、その日は誰もシフトに入れなくなります。
* 代理入力（追加）
  スマホ操作が苦手な方の代わりに、管理者が名前と属性を入力してシフトを追加してあげることができます。
* 強制削除
  予定の変更などで参加できなくなった方のシフトを、管理者権限で横の [✖️] ボタンから削除できます。
* 活動時間（分数）の変更
  通常は1回50分ですが、遅刻や早退などで活動時間が変わった場合、名前の横にある [⏱️] アイコンを押して分数を修正できます。この分数が月末の活動実績（CSV）に反映されます。

## 3. 月末の処理（実績集計とロック）
月末になったら、以下の手順で活動実績の報告準備を行います。

① 月のロック（確定）
サイドバーにある [🔴 〇月をロックする] ボタンを押します。
ロックすると、一般ユーザーはこれ以上その月のシフトを追加・削除できなくなり、掲示板の書き込みもできなくなります。（※いつでも「ロック解除」で元に戻せます）
ロック状態になると、画面下部に「活動実績（時間ランキング）」が表示されるようになります。

② 行政報告用 CSV出力
サイドバーの一番下にある [〇月分を集計・出力] ボタンを押します。
「📥 CSVをダウンロード」ボタンが出現するので、クリックして保存します。
ダウンロードしたExcel（CSV）ファイルには、「縦に名前と属性」「横に日付」が並んだマトリックス表と、個人の合計時間が自動計算されて出力されます。これをそのまま行政への報告等に活用してください。
"""

# --- Firebase初期化 ---
@st.cache_resource
def init_firebase():
    """Firebase Admin SDKを初期化する"""
    try:
        creds_dict = dict(st.secrets["firebase"])
        if "private_key" in creds_dict:
            creds_dict['private_key'] = creds_dict['private_key'].replace('\\n', '\n')

        creds = credentials.Certificate(creds_dict)
        
        if not firebase_admin._apps:
            firebase_admin.initialize_app(creds)
        return firestore.client()
    except Exception as e:
        st.error(f"Firebaseの初期化に失敗しました: {e}")
        return None

db = init_firebase()
if not db:
    st.stop()

# --- セッション状態の初期化 ---
if 'current_date' not in st.session_state:
    # 基準日を日本時間で取得する
    st.session_state.current_date = datetime.now(JST)
if 'admin_mode' not in st.session_state:
    st.session_state.admin_mode = False
if 'user_name' not in st.session_state:
    st.session_state.user_name = ""
if 'user_attribute' not in st.session_state:
    st.session_state.user_attribute = ""
if 'agreed_to_terms' not in st.session_state:
    st.session_state.agreed_to_terms = False

# --- データ取得・クリーンアップ関数 ---
@st.cache_data(ttl=60)
def get_firestore_data(year, month):
    """指定された月のFirestoreデータを取得する"""
    month_id = f"{year}-{month:02d}"
    
    events_ref = db.collection(EVENTS_COLLECTION)
    query = events_ref.where('month_id', '==', month_id)
    events = {doc.id: doc.to_dict() for doc in query.stream()}
    
    day_status_ref = db.collection(DAY_STATUS_COLLECTION)
    query = day_status_ref.where('month_id', '==', month_id)
    day_status = {doc.id: doc.to_dict() for doc in query.stream()}
    
    month_lock_doc = db.collection(MONTH_LOCKS_COLLECTION).document(month_id).get()
    is_month_locked = month_lock_doc.exists and month_lock_doc.to_dict().get('isLocked', False)

    board_ref = db.collection(BOARD_COLLECTION)
    # 絞り込みを外し、全メッセージを新しい順に取得する
    query = board_ref.order_by('timestamp', direction=firestore.Query.DESCENDING)
    board_messages = [doc.to_dict() for doc in query.stream()]

    return events, day_status, is_month_locked, board_messages

def cleanup_old_board_messages():
    """投稿から2週間以上経過した掲示板メッセージを削除する"""
    # 2週間前も日本時間基準で計算する
    two_weeks_ago = datetime.now(JST) - timedelta(weeks=2)
    old_messages_query = db.collection(BOARD_COLLECTION).where('timestamp', '<', two_weeks_ago).stream()
    
    batch = db.batch()
    deleted_count = 0
    for doc in old_messages_query:
        batch.delete(doc.reference)
        deleted_count += 1
    
    if deleted_count > 0:
        batch.commit()

# --- UIコンポーネントとロジック ---

def show_agreement_screen():
    """セキュリティ警告・同意画面を表示する"""
    st.markdown("## ⚠️ 重要なお知らせ")
    
    st.error("""
    **【セキュリティに関するご注意】**
    このアプリのURL（アドレス）は、**第三者には絶対に共有しないでください。**
    URLを知っている人は誰でもカレンダーを閲覧・編集できてしまいます。見守り活動に関わるメンバーだけで厳重に管理してください。
    """)
    
    st.warning("""
    **【ご利用対象者について】**
    このカレンダーを使用できるのは、**区へのサポーター登録が完了した方のみ**です。ご注意ください。
    """)
    
    st.write("上記の内容を理解し、同意しますか？")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("同意してアプリを利用する", type="primary", use_container_width=True):
            st.session_state.agreed_to_terms = True
            st.rerun()

def show_welcome_and_name_input():
    """ウェルカムメッセージと名前・属性入力フォームを表示する"""
    st.subheader("ようこそ！シフト管理を始めるには、お名前と属性を教えてください。お名前と属性のセットで個人を判別します。")
    st.info("💡 入力後はいつでもブラウザを閉じて終了できます。データは自動で保存されます。")
    
    with st.form("name_form"):
        name = st.text_input(
            "あなたのフルネームを入力してください", 
            placeholder="例：山田太郎",
            help="姓と名の間は詰めて入力してください。"
        )
        attribute = st.selectbox("あなたの属性を選択してください", options=list(USER_ATTRIBUTES.keys()))
        
        submitted = st.form_submit_button("利用開始")
        if submitted and name:
            st.session_state.user_name = name.replace(" ", "").replace("　", "")
            st.session_state.user_attribute = attribute
            st.rerun()
        elif submitted:
            st.warning("お名前を入力してください。")

def show_main_app():
    """メインのアプリケーションUIを表示する"""
    st.success(f"**{st.session_state.user_name}** さん（{st.session_state.user_attribute}）、こんにちは！")
    
    # --- 追加: 24時間以内の掲示板書き込みアラート ---
    year = st.session_state.current_date.year
    month = st.session_state.current_date.month
    _, _, _, board_messages = get_firestore_data(year, month)
    
    now_jst = datetime.now(JST)
    twenty_four_hours_ago = now_jst - timedelta(hours=24)
    
    recent_count = 0
    for msg in board_messages:
        ts = msg.get('timestamp')
        if ts and hasattr(ts, 'astimezone'):
            msg_time = ts.astimezone(JST)
            if msg_time >= twenty_four_hours_ago:
                recent_count += 1
                
    if recent_count > 0:
        alert_html = f"""
        <div style="
            padding: 15px;
            background-color: #fff0f0;
            border: 2px solid #ff4b4b;
            border-radius: 8px;
            margin-bottom: 20px;
            text-align: center;
            animation: pulse 2s infinite;
        ">
            <h4 style="color: #ff4b4b; margin: 0; font-weight: bold;">🚨 緊急連絡掲示板に24時間以内の新しい書き込みが {recent_count}件 あります！</h4>
            <p style="color: #ff4b4b; margin: 5px 0 0 0;">画面下部を必ずご確認ください。</p>
        </div>
        <style>
        @keyframes pulse {{
            0% {{ box-shadow: 0 0 0 0 rgba(255, 75, 75, 0.7); }}
            70% {{ box-shadow: 0 0 0 15px rgba(255, 75, 75, 0); }}
            100% {{ box-shadow: 0 0 0 0 rgba(255, 75, 75, 0); }}
        }}
        </style>
        """
        st.markdown(alert_html, unsafe_allow_html=True)
    # ----------------------------------------------
    
    with st.expander("📖 かんたんな使い方", expanded=False):
        st.markdown(f"""
        1. **シフトに入りたい日をクリック**: カレンダーで「開催日」となっている日付の「シフトに入る」ボタンを押します。
        2. **シフトを確認**: あなたの名前がカレンダーに表示されたら登録完了です。一日に最大{MAX_SHIFTS_PER_DAY}名まで登録できます。
        3. **シフトを削除**: 間違えて登録した場合は、自分の名前の横にある「✖️」ボタンを押すと削除できます。
        4. **掲示板の利用**: 緊急連絡掲示板にメッセージを書き込むことができます。
        """)
                # --- マニュアルダウンロードボタンを追加 ---
        st.download_button(
            label="📥 詳しい使い方マニュアルを保存する",
            data=USER_MANUAL_TEXT.encode('utf-8-sig'),
            file_name="見守りカレンダー_ユーザーマニュアル.txt",
            mime="text/plain"
        )
        # ----------------------------------------
    
    show_calendar()
    show_activity_record()
    show_board_and_info()

def show_calendar():
    """カレンダーのメインUIを描画する"""
    year = st.session_state.current_date.year
    month = st.session_state.current_date.month
    month_id = f"{year}-{month:02d}"

    events, day_status, is_month_locked, _ = get_firestore_data(year, month)

    header_cols = st.columns([1, 2, 1])
    if header_cols[0].button("<< 前の月"):
        st.session_state.current_date -= relativedelta(months=1)
        st.rerun()
    header_cols[1].header(f"{year}年 {month}月")
    if header_cols[2].button("次の月 >>"):
        st.session_state.current_date += relativedelta(months=1)
        st.rerun()

    # 属性カラーの凡例表示
    legend_html = " &nbsp;&nbsp;|&nbsp;&nbsp; ".join([f"<span style='color:{color}; font-weight:bold;'>■ {attr}</span>" for attr, color in USER_ATTRIBUTES.items()])
    st.markdown(f"<div style='text-align:center; padding: 10px; background-color: #f0f2f6; border-radius: 5px; margin-bottom: 10px;'>{legend_html}</div>", unsafe_allow_html=True)

    if is_month_locked:
        st.error("🔒 この月はロックされているため、シフトの編集や掲示板への書き込みはできません。")

    cal = calendar.monthcalendar(year, month)
    days_of_week = ["月", "火", "水", "木", "金", "土", "日"]
    
    st.divider()

    for week in cal:
        cols = st.columns(7)
        for i, day in enumerate(week):
            if day == 0:
                cols[i].write("")
                continue
            
            day_name = days_of_week[i]
            date_str = f"{month_id}-{day:02d}"
            is_held = day_status.get(date_str, {}).get('isHeld', False)
            day_events = [data for data in events.values() if data.get('date') == date_str]
            is_full = len(day_events) >= MAX_SHIFTS_PER_DAY
            
            with cols[i].container(border=True):
                color = "red" if day_name == "日" else "blue" if day_name == "土" else "inherit"
                full_mark = " <span style='color:red; font-size:0.8em;'>🈵</span>" if is_held and is_full else ""
                st.markdown(f"<p style='color:{color}; margin-bottom:0; text-align:center;'><strong>{day}</strong> ({day_name}){full_mark}</p>", unsafe_allow_html=True)

                if st.session_state.admin_mode:
                    new_is_held = st.checkbox("開催", value=is_held, key=f"held_{date_str}", disabled=is_month_locked)
                    if new_is_held != is_held:
                        db.collection(DAY_STATUS_COLLECTION).document(date_str).set({'isHeld': new_is_held, 'month_id': month_id})
                        st.cache_data.clear(); st.rerun()
                elif is_held:
                    st.success("開催日")

                # シフトリスト表示（属性による色分け、分数の表示）
                for event in day_events:
                    doc_id = [k for k, v in events.items() if v == event][0]
                    attr = event.get('attribute', 'その他')
                    text_color = USER_ATTRIBUTES.get(attr, 'gray')
                    minutes = event.get('minutes', MINUTES_PER_SHIFT)
                    
                    # 自分が登録したシフトかどうかを判定（名前と属性の両方が一致するか）
                    is_own_shift = (event.get('name') == st.session_state.user_name and attr == st.session_state.user_attribute)
                    
                    if st.session_state.admin_mode and not is_month_locked:
                        shift_cols = st.columns([5, 2, 2])
                        # 名前と現在の分数を表示
                        time_color = "red" if minutes != MINUTES_PER_SHIFT else "gray"
                        shift_cols[0].markdown(f"<div style='line-height:1.2;'><span style='color:{text_color}; font-size:0.9em;'>👤 {event.get('name')}</span><br><span style='color:{time_color}; font-size:0.8em;'>({minutes}分)</span></div>", unsafe_allow_html=True)
                        
                        # 管理者用の分数変更ポップオーバー
                        with shift_cols[1].popover("⏱️", help="活動時間を変更"):
                            st.write(f"**{event.get('name')}** さんの時間")
                            new_min = st.number_input("分数", value=minutes, step=5, min_value=0, key=f"min_{doc_id}")
                            if new_min != minutes:
                                db.collection(EVENTS_COLLECTION).document(doc_id).update({'minutes': new_min})
                                st.cache_data.clear(); st.rerun()
                                
                        # 削除ボタン
                        if shift_cols[2].button("✖️", key=f"del_{doc_id}", help="削除"):
                            db.collection(EVENTS_COLLECTION).document(doc_id).delete()
                            st.cache_data.clear(); st.rerun()
                    else:
                        shift_cols = st.columns([4, 1])
                        # 一般ユーザー向けの表示
                        display_name = f"**{event.get('name')}**" if is_own_shift else f"{event.get('name')}"
                        time_color = "red" if minutes != MINUTES_PER_SHIFT else "gray"
                        shift_cols[0].markdown(f"<span style='color:{text_color}; font-size:0.9em;'>👤 {display_name}</span> <span style='color:{time_color}; font-size:0.8em;'>({minutes}分)</span>", unsafe_allow_html=True)
                        
                        if is_own_shift and not is_month_locked:
                            if shift_cols[1].button("✖️", key=f"del_{doc_id}", help="削除"):
                                db.collection(EVENTS_COLLECTION).document(doc_id).delete()
                                st.cache_data.clear(); st.rerun()
                
                if is_held and not is_month_locked:
                    if not is_full:
                        if st.session_state.admin_mode:
                            with st.form(key=f"admin_add_form_{date_str}"):
                                admin_add_name = st.text_input("代理入力", key=f"admin_name_{date_str}", label_visibility="collapsed", placeholder="名前")
                                admin_add_attr = st.selectbox("属性", options=list(USER_ATTRIBUTES.keys()), key=f"admin_attr_{date_str}", label_visibility="collapsed")
                                if st.form_submit_button("追加"):
                                    if admin_add_name:
                                        new_event = {
                                            'date': date_str, 'month_id': month_id,
                                            'name': admin_add_name,
                                            'attribute': admin_add_attr,
                                            'minutes': MINUTES_PER_SHIFT,
                                            'createdAt': firestore.SERVER_TIMESTAMP,
                                            'uid': str(uuid.uuid4())
                                        }
                                        db.collection(EVENTS_COLLECTION).add(new_event)
                                        st.cache_data.clear(); st.rerun()
                        else:
                            if st.button("シフトに入る", key=f"add_{date_str}"):
                                # 名前と属性の両方が一致するデータが既にあるかチェック
                                is_already_in = any(e.get('name') == st.session_state.user_name and e.get('attribute', 'その他') == st.session_state.user_attribute for e in day_events)
                                if not is_already_in:
                                    new_event = {
                                        'date': date_str, 'month_id': month_id,
                                        'name': st.session_state.user_name,
                                        'attribute': st.session_state.user_attribute,
                                        'minutes': MINUTES_PER_SHIFT,
                                        'createdAt': firestore.SERVER_TIMESTAMP,
                                        'uid': str(uuid.uuid4())
                                    }
                                    db.collection(EVENTS_COLLECTION).add(new_event)
                                    st.cache_data.clear(); st.rerun()
                                else:
                                    st.warning("すでに入っています。")
                    else:
                        st.error("🈵 満員です")

def show_activity_record():
    """活動実績の集計結果を表示する"""
    year = st.session_state.current_date.year
    month = st.session_state.current_date.month
    
    st.divider()
    st.subheader(f"⏱️ {year}年{month}月の活動実績")
    
    events, _, is_month_locked, _ = get_firestore_data(year, month)
    
    if not is_month_locked:
        st.info(f"ℹ️ {month}月のシフトはまだ管理者によってロック（確定）されていないため、活動実績は表示されません。")
        return
    
    # 属性を含めて集計する（同姓同名でも属性が違えば別として扱う）
    user_data = {}
    for event in events.values():
        name = event.get('name')
        attr = event.get('attribute', 'その他')
        if name:
            # 名前と属性の組み合わせをキーにする
            key = (name, attr)
            if key not in user_data:
                user_data[key] = {
                    'お名前': name,
                    '属性': attr,
                    '活動時間(分)': 0
                }
            user_data[key]['活動時間(分)'] += event.get('minutes', MINUTES_PER_SHIFT)
            
    if not user_data:
        st.write("この月の活動記録はありません。")
    else:
        # 表に表示しやすい形（リスト）に変換
        data_list = list(user_data.values())
            
        df = pd.DataFrame(data_list)
        df = df.sort_values(by=['活動時間(分)', 'お名前'], ascending=[False, True]).reset_index(drop=True)
        # hide_index=True を指定して、一番左の連番（0, 1, 2...）を非表示にする
        st.dataframe(df, use_container_width=True, hide_index=True)

def show_board_and_info():
    """掲示板と説明セクションを表示する"""
    year = st.session_state.current_date.year
    month = st.session_state.current_date.month
    month_id = f"{year}-{month:02d}"
    _, _, is_month_locked, board_messages = get_firestore_data(year, month)

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📢 緊急連絡掲示板")
        st.info("💡 投稿から2週間が経過したメッセージは自動的に削除されます。")
        
        with st.form("board_form", clear_on_submit=True):
            name_input = st.text_input("お名前", value=st.session_state.user_name)
            message_input = st.text_area("メッセージ")
            if st.form_submit_button("書き込む"):
                if name_input and message_input:
                    new_message = {
                        'month_id': month_id, 'name': name_input,
                        'message': message_input, 'timestamp': firestore.SERVER_TIMESTAMP
                    }
                    db.collection(BOARD_COLLECTION).add(new_message)
                    st.cache_data.clear(); st.rerun()
                else:
                    st.warning("お名前とメッセージを入力してください。")
        
        for msg in board_messages:
            ts = msg.get('timestamp')
            # Firestoreのタイムスタンプを日本時間(JST)に変換して表示
            if ts and hasattr(ts, 'astimezone'):
                timestamp_str = ts.astimezone(JST).strftime('%Y-%m-%d %H:%M')
            else:
                timestamp_str = "時刻不明"
            
            st.markdown(f"""
            <div style="border-bottom: 1px solid #e0e0e0; padding-bottom: 8px; margin-bottom: 8px;">
                <p style="margin: 0;"><strong>{msg.get('name')}</strong> <small>({timestamp_str})</small></p>
                <p style="margin: 0; white-space: pre-wrap;">{msg.get('message')}</p>
            </div>
            """, unsafe_allow_html=True)

    with col2:
        st.subheader("💡 ご利用上のルール")
        st.warning("""
        - シフトは「早いもの勝ち」で決めていきます。
        - 3名以上の参加がない場合は、開催を取り消すことがあります。
        - **管理者が調整のため、シフトの追加や削除を行う場合があります。シフト確定後は、ご自身で最終確認をお願いします。**
        """)

def show_admin_sidebar():
    """管理者用のサイドバーと機能を表示する"""
    with st.sidebar:
        st.title("🛠️ 管理者メニュー")
        
        if not st.session_state.admin_mode:
            password = st.text_input("パスワード", type="password")
            if st.button("ログイン"):
                if password == st.secrets["admin"]["password"]:
                    st.session_state.admin_mode = True; st.rerun()
                else:
                    st.error("パスワードが違います。")
        
        if st.session_state.admin_mode:
            st.success("管理者としてログイン中")
            if st.button("ログアウト"):
                st.session_state.admin_mode = False; st.rerun()

            # --- 管理者用マニュアルダウンロードボタンを追加 ---
            st.download_button(
                label="📥 管理者マニュアルを保存する",
                data=ADMIN_MANUAL_TEXT.encode('utf-8-sig'),
                file_name="見守りカレンダー_管理者マニュアル.txt",
                mime="text/plain",
                use_container_width=True
            )
            # ---------------------------------------------

            st.divider()

            year = st.session_state.current_date.year
            month = st.session_state.current_date.month
            month_id = f"{year}-{month:02d}"
            _, _, is_month_locked, _ = get_firestore_data(year, month)
            
            st.subheader("月のロック管理")
            if is_month_locked:
                if st.button(f"🔓 {month}月をロック解除"):
                    db.collection(MONTH_LOCKS_COLLECTION).document(month_id).set({'isLocked': False})
                    st.cache_data.clear(); st.rerun()
            else:
                if st.button(f"🔴 {month}月をロックする"):
                    db.collection(MONTH_LOCKS_COLLECTION).document(month_id).set({'isLocked': True})
                    st.cache_data.clear(); st.rerun()

            st.divider()

            st.subheader("📊 行政報告用 CSV出力")
            st.write("現在表示中の月の集計データを出力します。")
            if st.button(f"{month}月分を集計・出力"):
                generate_admin_csv(year, month)

def generate_admin_csv(year, month):
    """行政報告用のマトリックス形式CSVを生成する"""
    month_id = f"{year}-{month:02d}"
    with st.spinner("集計中..."):
        events_ref = db.collection(EVENTS_COLLECTION).where('month_id', '==', month_id).stream()
        events_list = [doc.to_dict() for doc in events_ref]
        
        if not events_list:
            st.sidebar.warning(f"{month}月のシフトデータがありません。")
            return
            
        df = pd.DataFrame(events_list)
        
        if 'minutes' not in df.columns:
            df['minutes'] = MINUTES_PER_SHIFT
        else:
            df['minutes'] = df['minutes'].fillna(MINUTES_PER_SHIFT)
            
        # 古いデータで属性がない場合への対応
        if 'attribute' not in df.columns:
            df['attribute'] = 'その他'
        else:
            df['attribute'] = df['attribute'].fillna('その他')
        
        # 'date'（例: 2026-02-01）から「日」と「曜日」を抽出
        df['date_dt'] = pd.to_datetime(df['date'])
        df['日'] = df['date_dt'].dt.day
        
        days_map = {0: '月', 1: '火', 2: '水', 3: '木', 4: '金', 5: '土', 6: '日'}
        df['曜日'] = df['date_dt'].dt.weekday.map(days_map)
        
        # 列名を見やすく変更
        df = df.rename(columns={'name': 'お名前', 'attribute': '属性'})
        
        # クロス集計（縦：[お名前, 属性]の2段組み、横：[日, 曜日]の2段組み、値：活動時間）
        pivot = df.pivot_table(index=['お名前', '属性'], columns=['日', '曜日'], values='minutes', aggfunc='sum', fill_value=0)
        
        # カレンダー通りに左から1日、2日…と並び替える
        pivot = pivot.sort_index(axis=1, level=0)
        
        # 個人別の合計分数（行の合計）を追加
        pivot[('合計(分)', '')] = pivot.sum(axis=1)
        
        st.sidebar.success("集計完了！下のボタンからダウンロードしてください。")
        
        # Excelで文字化けしないように 'utf-8-sig' でエンコード
        csv = pivot.to_csv().encode('utf-8-sig')
        st.sidebar.download_button(
            label="📥 CSVをダウンロード",
            data=csv,
            file_name=f"admin_report_{month_id}.csv",
            mime="text/csv"
        )

# --- メイン実行部分 ---
if __name__ == "__main__":
    if not st.session_state.agreed_to_terms:
        show_agreement_screen()
    else:
        st.title("🗓️ 見守りシフト管理カレンダー")
        st.caption("管理者の方は、画面左上の「>」をクリックしてメニューを開いてください。")
        
        show_admin_sidebar()

        if 'cleanup_done' not in st.session_state:
            cleanup_old_board_messages()
            st.session_state.cleanup_done = True

        if not st.session_state.user_name:
            show_welcome_and_name_input()
        else:
            show_main_app()