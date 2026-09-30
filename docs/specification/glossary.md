# 用語集

`docs/specification/` 配下の仕様書で使う語を統一するための辞書。仕様書ではこの表の「採用する語」を使い、「使わない言い換え」は使わない。新しい語を導入するときはこの表に追加する

- 英語のまま使う語は、初出時に括弧で意味を添えてよい (例: Affected Rows (影響を受けた行数))
- メッセージ名 (`StmtExecute`、`ColumnMetaData` など) と `.proto` 上の識別子は訳さず、バッククォートで書く

## 全般

| 採用する語 | 使わない言い換え | 英語 / ソース上の名前 | 意味・使い分け |
| --- | --- | --- | --- |
| MineSQL | minesql、mineSQL | MineSQL (リポジトリ名とパスは `minesql`) | この仕様書が対象とする、MySQL を参考に作るデータベースの名前。文中の表記は MineSQL に統一し、小文字の minesql はリポジトリ名・パス・URL と、minesql スキーマの名前にだけ使う |

## プロトコル

| 採用する語 | 使わない言い換え | 英語 / ソース上の名前 | 意味・使い分け |
| --- | --- | --- | --- |
| X Protocol | X プロトコル | X Protocol | 通信規約そのもの (フレーム + protobuf メッセージ)。`.proto` ファイルが仕様の正 |
| フレーム | パケット、メッセージ (送受信の単位を指すとき) | frame | 送受信の単位 (`length` + `message_type` + `message_payload`) |
| メッセージ | | message | フレームに載る protobuf のメッセージ 1 つ |
| リクエスト | 要求 | request | クライアントからサーバーへのメッセージのうち、シーケンスを開始するもの |
| レスポンス | 応答 | response | リクエストに対してサーバーが返すメッセージ (の列) |
| シーケンス | やり取り、メッセージ列 | message sequence | 1 つのリクエストと、それに続くレスポンスの列 |
| 取得完了メッセージ | 取得完了、リザルトセットの終わり、FetchDone (地の文で) | `FetchDone` | 1 つのリザルトセットの行をすべて送ったことを示すメッセージ。シーケンスはまだ終わらず、Notice と終端メッセージが続く |
| 終端メッセージ | 終端、完了メッセージ | end-state | シーケンスを終わらせるメッセージ (`StmtExecuteOk`、`Ok`、`AuthenticateOk`、`Error`)。取得完了メッセージ (`FetchDone`) は含めない |
| Notice | 通知 | notice | サーバーが送る、リクエストへの直接のレスポンスではないメッセージ (`Mysqlx.Notice.Frame`) |
| 警告 | ワーニング | warning | `Warning` 型の Notice の中身。「警告の Notice」と書く |
| capability | 機能、ケイパビリティ | capability | 接続で何ができるかを表す名前つきの値 |
| 安全な接続 | セキュア接続、暗号化接続 | secure connection (`Connection_type_helper::is_secure_type`) | TLS に切り替えた接続、または Unix ソケット接続。`PLAIN` 認証はここでだけ使える |
| 認証メカニズム | 認証方式、認証プラグイン | authentication mechanism (`mech_name`) | `MYSQL41` / `PLAIN` / `SHA256_MEMORY` |
| namespace | 名前空間 | namespace (`StmtExecute.namespace`) | `sql` / `mysqlx` |
| 管理コマンド | アドミンコマンド | admin command | `mysqlx` namespace で実行するコマンド |
| リザルトセット | 結果セット | resultset | `ColumnMetaData` の列と `Row` の列からなる、1 つの結果の集まり |
| 深刻度 | 重大度、レベル | severity (`ERROR` / `FATAL`) | `Error` メッセージがシーケンスだけを中断するか、接続を閉じるか |
| Expect ブロック | 期待ブロック、エクスペクテーション | expectation block | `Expect.Open` から `Expect.Close` までの、条件つきで実行するリクエストの範囲 |

## SQL の実行

| 採用する語 | 使わない言い換え | 英語 / ソース上の名前 | 意味・使い分け |
| --- | --- | --- | --- |
| ステートメント | 文、クエリ | statement | 実行する SQL の 1 単位。SQL であることを強調するときは「SQL ステートメント」 |
| 文脈化 | コンテキスト化、contextualize (地の文で) | contextualization (`contextualize`) | MySQL のパーサーで、parse tree を根から辿り、各ノードを文脈 (どのクエリブロックの中か、内部セッションの実行状態) に当てはめて AST を組み立てる段階。MineSQL は持たず、構文解析が AST を直接作る |
| AST | 構文の木、木 | abstract syntax tree (MySQL では parse tree と、文脈化後の AST を区別する) | パーサーの出力。MineSQL では MySQL の `Query_block` / `Item` に相当する構造を未解決の状態で作ったもので、プリペアが同じノードに解決結果を書き込み、オプティマイザとエグゼキュータも同じ構造を使う |
| Tree | 木、木構造 | tree | ノードの入れ子で表した構造。「構文木」「式の Tree」のように使う |
| クエリブロック | SELECT 単位、クエリ単位 | query block (`Query_block`) | 1 つの SELECT に対応する意味の単位 (テーブル一覧、select list、条件、グループ化、並び替え、件数制限) |
| 未解決 | 未束縛、unresolved | unresolved | AST のノードのうち、書かれた名前がどのテーブル・どのカラムを指すか、式の値の型が何かがまだ決まっていない状態。プリペアが決めると解決済みになる |
| プリペア | 準備、準備フェーズ、resolve | prepare (`Sql_cmd_dml::prepare`) | AST に対して名前解決と型決定を行う段階。データディクショナリを参照するのはここ |
| オプティマイザ | 最適化 (段階を指すとき)、最適化器、プランナー | optimizer (Query Optimizer) | 解決済みの AST から実行計画を選ぶモジュールと、その段階 |
| エグゼキュータ | 実行 (段階を指すとき)、実行器、実行エンジン | executor (Query Executor) | 実行計画を実行し、結果を返すモジュールと、その段階。実行コマンドの実行の操作のうち DML の部分を担う |
| 実行コマンド | コマンドオブジェクト、ステートメントオブジェクト | `Sql_cmd` | ステートメント 1 つのプリペアと実行の操作を持つオブジェクト。パーサーの出口で AST を包んで作り、内部セッションがこれの `prepare` → `execute` を呼ぶ |
| パラメータ | 引数 (プレースホルダに入る値を指すとき) | `StmtExecute.args` (`sql` namespace) | ステートメント中のプレースホルダ `?` を置き換える値。管理コマンドに渡す名前付きの値は「引数」と呼ぶ |
| プレースホルダ | ワイルドカード | placeholder (`?`) | ステートメント中でパラメータが入る位置 |
| Affected Rows | 影響行数、影響を受けた行数 | `ROWS_AFFECTED` | ステートメントが変更した行数。Notice の `SessionStateChanged` で送られる |
| Last Insert ID | 生成された ID、自動採番値、auto-generated ID | `GENERATED_INSERT_ID` (X Protocol の Notice)、`last_insert_id` (classic の OK パケット)、`LAST_INSERT_ID()` | 直前のステートメントで AUTO_INCREMENT により生成された値。Notice の `SessionStateChanged` で送られる |
| 実行ステータス | 実行結果の付帯情報 | (`ROWS_AFFECTED` / `GENERATED_INSERT_ID` / `PRODUCED_MESSAGE`) | リザルトセットとは別に Notice で届く、実行に関する情報の総称 |
| 内部セッション | THD、サーバーセッション | internal session (`srv_session`、実装上は THD) | セッションごとに 1 つある、ステートメントの実行の入口と実行の文脈。実行ユーザー、既定スキーマ、トランザクションの状態、診断、kill の要求を保持し、SQL はこの文脈で実行される |
| 実行の文脈 | セッション状態、コンテキスト | (THD が持つ状態) | 内部セッションが持つ、実行ユーザーと全体権限、既定スキーマ、トランザクションの状態、診断 (エラーと警告)、kill の要求の集まり |
| 実行ユーザー | 身元、セキュリティコンテキスト (MySQL の用語) | security context (`priv_user`) | 内部セッションが SQL を実行するときの利用者。認証の成功後に設定され、権限の判定や `CURRENT_USER()` の値になる |
| データディクショナリ | 辞書、カタログ (単独では使わない)、メタデータストア | data dictionary (`sql/dd`) | スキーマ・テーブル・カラム・インデックスの定義を保持し、名前解決と DDL に提供する部分。アカウントはシステムテーブルに置き、ここには含めない |
| ディクショナリテーブル | DD テーブル、データディクショナリテーブル | dictionary table (`mysql.tables` など) | 定義を行として持つテーブル。MySQL では mysql スキーマ、MineSQL では minesql スキーマにあり、利用者からは見えない。`user` などのシステムテーブルとは別物 |
| ディクショナリオブジェクト | DD オブジェクト、メタデータオブジェクト | dictionary object (`dd::Schema`、`dd::Table`) | ディクショナリテーブルの行を組み立てた、定義のメモリ上の表現。テーブルのオブジェクトはカラムとインデックスを含む |
| オブジェクトキャッシュ | ディクショナリキャッシュ、DD キャッシュ | shared dictionary cache (`Shared_dictionary_cache`) | 読み込み済みのディクショナリオブジェクトを全ての接続で共有して保持するキャッシュ |
| ディクショナリクライアント | キャッシュクライアント、DD クライアント | dictionary client (`Dictionary_client`) | 定義の取得・保存・変更・削除の操作をまとめたもので、内部セッションごとに 1 つある |
| 起動時の初期化 | bootstrap (地の文で)、ブートストラップ | bootstrap (`dd::bootstrap`) | 初回の起動でディクショナリテーブルと minesql スキーマ (MySQL では mysql) を作り、2 回目以降は既存のディクショナリテーブルを開いて定義を読める状態にする手順 |
| 原子的な DDL | Atomic DDL、アトミック DDL | atomic DDL | 1 つの DDL のディクショナリテーブルの変更とストレージエンジンの操作を、まとめて確定するか、まとめて残さないかのどちらかにすること。利用者のトランザクションに DDL を含められること (トランザクショナルな DDL) とは別 |
| 暗黙のコミット | 自動コミット (この意味では使わない) | implicit commit | DDL などの実行前後に、そのセッションで進行中のトランザクションが自動的にコミットされること |
| DDL ログ | | DDL log (`mysql.innodb_ddl_log`) | 原子的な DDL のために、ストレージエンジン側の戻せない操作を記録しておく隠しテーブル。コミット後の後始末と、失敗やクラッシュ後の取り消しに使う |
| テーブル | 表 | table | 行とカラムからなるデータの入れ物で、スキーマに属する。文書内の表 (markdown の表) は「表」、tablespace は「表領域」と書き、この語は使わない |
| カラム | 列、フィールド | column | テーブルを構成する縦の項目 (行と対)。文書内の表の縦の項目を指すときは「欄」と書く |
| 行 | レコード (ページ上の物理形式を指すとき以外)、タプル | row (`Row` メッセージ、Affected Rows) | テーブルの横 1 件のデータ。SQL、プロトコル、ディクショナリの文脈で使う。文書内の表の行は「表の行」(DB のものは「テーブルの行」) と書けば区別できる |
| レコード | 行 (ページ上の物理形式を指すとき) | record (InnoDB の `rec_t`) | 行をページに格納した物理形式で、ストレージの文脈で使う。SQL、プロトコル、ディクショナリの文脈では「行」を使う |
| スキーマ | データベース (MySQL では `SCHEMA` と `DATABASE` は同義) | schema / database | テーブルの名前空間。システムテーブルは minesql スキーマに置く |
| mysql スキーマ | システムスキーマ、mysql データベース、mysql システムスキーマ (マニュアルの節名) | the mysql system schema | MySQL がシステムテーブルとディクショナリテーブルを置くスキーマ (MineSQL の対応物は minesql スキーマ)。MySQL のマニュアルが system schema と呼ぶのはこのスキーマで、監視用のビューを集めた `sys` スキーマや `information_schema` / `performance_schema` とは別物 |
| minesql スキーマ | システムスキーマ、mysql スキーマ (MineSQL のものを指すとき) | | MineSQL がシステムテーブル (`user`) とディクショナリテーブルを置くスキーマ。MySQL の mysql スキーマに相当し、名前は写さない |
| 既定スキーマ | カレントスキーマ、デフォルトデータベース | default schema (`AuthenticateStart.schema`、Notice の `CURRENT_SCHEMA`) | 修飾のないテーブル名を解決するスキーマ。接続時に決まる |
| システムテーブル | システムテーブル、メタデータテーブル | system table (`mysql.user` など) | サーバー自身が使うテーブル。普通のテーブルとして同じストレージに置く |
| 名前解決の文脈 | スコープ、名前空間 (名前解決の話をするとき) | name resolution context (`Name_resolution_context`) | クエリブロックごとに持つ、見えているテーブルの一覧。カラム名を探す範囲で、外側の文脈を辿れる |
| 型決定 | 型解決、型推論 | type resolution (`resolve_type`) | 式の各ノードの型を、型の規則に従って決める段階。プリペアが行う |
| 結合の単純化 | 結合の平坦化、ON の WHERE への移動 | join simplification | プリペアの最後に、内部結合の ON 条件を WHERE に移し、FROM の入れ子を平坦なテーブルの並びにする変換。MySQL では外部結合の内部結合化も同じ段階で行う |
| ロック読み取り | ロック付き読み取り、ロッキングリード | locking read (`SELECT ... FOR UPDATE`) | 読んだ行にロックを取る SELECT。`FOR UPDATE` は排他ロック、`FOR SHARE` (同義語 `LOCK IN SHARE MODE`) は共有ロックを取る |

## 接続とディスパッチ

| 採用する語 | 使わない言い換え | 英語 / ソース上の名前 | 意味・使い分け |
| --- | --- | --- | --- |
| クライアント | | client | プロトコルの端点としての、接続してくる側 |
| クライアントプログラム | | | X Plugin 内部の「接続 (Client)」と同じ図に登場するときの、接続してくる側の呼び方 |
| サーバー | サーバー本体 | server | プロトコルの端点としての、接続を受ける側 (X Plugin を含む mysqld 全体) |
| 接続 | コネクション、クライアント (X Plugin 内部の要素を指すとき) | connection (X Plugin の実装上は `Client`) | 受け付けた TCP / Unix ソケット接続 1 本と、それに対応するサーバー側の要素 |
| セッション | | session (`Session`) | 認証を経て確立する、利用者としてコマンドを実行する文脈 |
| コネクションハンドラー | 接続層、コネクションハンドラ | connection handler | 接続の受付からセッションの確立・切断までを担う部分 |
| ハンドラ | 種別ごとの処理、ハンドラー | handler | ディスパッチャがリクエストの種別ごとに持つ処理の単位 (SQL ステートメント、管理コマンド、Expect ブロック)。認証メカニズムのものは「認証ハンドラ」と呼び分ける |
| 認証ハンドラ | 認証プラグイン (X Plugin 側を指すとき)、SASL ハンドラ | authentication handler (`iface::Authentication`、`Sasl_*_auth`) | 認証メカニズム 1 つ分のやり取り (チャレンジの生成、応答の受け取り) を進める部品。`AuthenticateStart` ごとに作られる |
| 監査イベント | 監査通知、audit event | audit event (`MYSQL_AUDIT_*`) | MySQL がサーバー内の出来事 (接続、ステートメントの実行、アカウントの変更など) を登録済みのプラグインに知らせる通知。X Plugin はアカウントの変更の通知を受けて SHA256 パスワードキャッシュのエントリを消す。MineSQL はこの仕組みを持たない |
| アカウント照合 | 認証チェック、資格情報の検証 | account verification (`Account_verification_handler`) | 資格情報をアカウント情報 (MySQL では `mysql.user`) と突き合わせる処理 |
| SHA256 パスワードキャッシュ | パスワードキャッシュ | `SHA256_password_cache` | `SHA256_MEMORY` の照合に使う、利用者ごとのパスワードのハッシュを保持するサーバー内のキャッシュ |
| アカウント | ユーザー (アカウントを指すとき) | account (`mysql.user` の 1 行、`'user'@'host'`) | 利用者の名前とホストの組に、認証文字列と全体権限を結びつけたもの。MineSQL のホストは `%` のみ |
| アカウント管理ステートメント | アカウント文、ユーザー管理文、DCL | account management statements (`CREATE USER` / `ALTER USER` / `DROP USER`) | アカウントを作成・変更・削除するステートメント。`GRANT` / `REVOKE` は含めない |
| 全体権限 | グローバル権限、静的権限 | global privileges (`mysql.user` の `*_priv` カラム、`GRANT ... ON *.*`) | スキーマやテーブルを限定しない権限。MineSQL が持つ唯一の粒度で、認証の成功時に実行ユーザーに設定する |
| デリゲート | 結果の受け口、コールバック | command delegate (`ngs::Command_delegate`、`Streaming_command_delegate`) | ステートメントの実行 1 回につき作られ、内部セッションからのコールバック (カラム定義、行、完了、エラー) を受けてプロトコルのメッセージに変換して送る部品 |
| コマンドディスパッチャ | ディスパッチャ (初出時)、コマンドディスパッチャー | command dispatcher (`Dispatcher`) | セッションが受け取ったリクエストを振り分け、内部セッションに委ねて、レスポンスを返す部分 |
| 受け付ける / 受付 | accept する、アクセプト | accept | 接続を受け入れること。動詞は「受け付ける」、名詞は「受付」 |
| 切断 | 接続を閉じる (曖昧なとき) | | TCP / Unix ソケット接続を閉じること。セッションを閉じることには使わない |
| セッションを閉じる | セッションの切断 | `Session.Close` | 接続を維持したままセッションだけを終えること |
| kill | 強制終了 | kill (`kill_client`、`KILL` ステートメント) | 他の接続 (または自分自身) を強制的に閉じる操作 |
| acceptor スレッド | 受付スレッド、リスナースレッド | acceptor (スケジューラ名 `network`) | listen ソケットのイベントループを回すスレッド |
| ワーカースレッド | 作業スレッド | worker (スケジューラ名 `work`) | 接続 1 本の処理を担うスレッド |
| Expect スタック | | `Expectation_stack` | セッションごとに持つ、開いている Expect ブロックの入れ子 |

## SQL の構文

| 採用する語 | 使わない言い換え | 英語 / ソース上の名前 | 意味・使い分け |
| --- | --- | --- | --- |
| トークン | 字句、語彙素 | token | 字句解析器が返す最小の単位 (キーワード、識別子、リテラル、記号) |
| キーワード | 予約語 (非予約語を含めて指すとき) | keyword (`lex.h` の `symbols`) | キーワード表に載っている語。予約語と非予約語に分かれる |
| 予約語 | | reserved word | キーワードのうち、引用しないと識別子に使えない語 |
| 非予約語 | | non-reserved keyword (`ident_keyword`) | キーワードのうち、識別子としても使える語 |
| 識別子 | 名前 (構文の話をするとき) | identifier (`IDENT` / `IDENT_QUOTED`) | テーブル名、カラム名、別名などを表す語。引用なしと引用あり (バッククォート) がある |
| リテラル | 定数、即値 | literal | 文字列、数値、真偽値をそのまま書いた値 |
| select list | 選択リスト、SELECT リスト、射影、出力カラム | select list | `SELECT` と `FROM` の間に並べた、結果のカラムになる式の並び (`SELECT name, id + 1 FROM users` の `name, id + 1`) |
| 句 | 節 | clause | ステートメントを構成する単位 (SELECT の select list、FROM、WHERE など)。MineSQL の AST はこの単位で構成する |
| 規則 | プロダクション、生成規則 | grammar rule (`sql_yacc.yy` の非終端記号の定義) | 文法ファイルの 1 つの定義。複数の選択肢を持つ |
| 選択肢 | 代替、右辺 | alternative | 規則の中で `\|` で区切られた 1 つの形 |
| 優先順位 | 結合の強さ | precedence (`%left` / `%right` / `%nonassoc`) | 演算子どうしの結び付きの順序。同じ順位での左右の結び付き方は結合性と呼ぶ |
