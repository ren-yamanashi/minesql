# 用語集

`docs/specification/` 配下の仕様書で使う語を統一するための辞書。仕様書ではこの表の「採用する語」を使い、「使わない言い換え」は使わない。新しい語を導入するときはこの表に追加する

- 英語のまま使う語は、初出時に括弧で意味を添えてよい (例: Affected Rows (影響を受けた行数))
- メッセージ名 (`StmtExecute`、`ColumnMetaData` など) と `.proto` 上の識別子は訳さず、バッククォートで書く

## プロトコル

| 採用する語 | 使わない言い換え | 英語 / ソース上の名前 | 意味・使い分け |
| --- | --- | --- | --- |
| フレーム | パケット、メッセージ (送受信の単位を指すとき) | frame | 送受信の単位 (`length` + `message_type` + `message_payload`) |
| メッセージ | | message | フレームに載る protobuf のメッセージ 1 つ |
| リクエスト | 要求 | request | クライアントからサーバーへのメッセージのうち、シーケンスを開始するもの |
| レスポンス | 応答 | response | リクエストに対してサーバーが返すメッセージ (の列) |
| シーケンス | やり取り、メッセージ列 | message sequence | 1 つのリクエストと、それに続くレスポンスの列 |
| 終端 | 完了メッセージ | end-state | シーケンスを終わらせるメッセージ (`StmtExecuteOk`、`Ok`、`Error`) |
| Notice | 通知 | notice | サーバーが送る、リクエストへの直接のレスポンスではないメッセージ (`Mysqlx.Notice.Frame`) |
| 警告 | ワーニング | warning | `Warning` 型の Notice の中身。「警告の Notice」と書く |
| capability | 機能、ケイパビリティ | capability | 接続で何ができるかを表す名前つきの値 |
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
| パラメータ | 引数 (プレースホルダに入る値を指すとき) | `StmtExecute.args` (`sql` namespace) | ステートメント中のプレースホルダ `?` を置き換える値。管理コマンドに渡す名前付きの値は「引数」と呼ぶ |
| プレースホルダ | ワイルドカード | placeholder (`?`) | ステートメント中でパラメータが入る位置 |
| Affected Rows | 影響行数、影響を受けた行数 | `ROWS_AFFECTED` | ステートメントが変更した行数。Notice の `SessionStateChanged` で送られる |
| Last Insert ID | 生成された ID、自動採番値、auto-generated ID | `GENERATED_INSERT_ID` (X Protocol の Notice)、`last_insert_id` (classic の OK パケット)、`LAST_INSERT_ID()` | 直前のステートメントで AUTO_INCREMENT により生成された値。Notice の `SessionStateChanged` で送られる |
| 実行ステータス | 実行結果の付帯情報 | (`ROWS_AFFECTED` / `GENERATED_INSERT_ID` / `PRODUCED_MESSAGE`) | リザルトセットとは別に Notice で届く、実行に関する情報の総称 |
| SQL 層 | コアサーバー、サーバー本体、SQL エンジン | | パーサー以降の、ステートメントを解析・最適化・実行する層 |
| 内部セッション | THD、サーバーセッション | internal session (`srv_session`、実装上は THD) | 利用者の身元と実行状態を持ち、その利用者として SQL を実行する SQL 層側の場 |

## 接続とディスパッチ

| 採用する語 | 使わない言い換え | 英語 / ソース上の名前 | 意味・使い分け |
| --- | --- | --- | --- |
| クライアント | | client | プロトコルの端点としての、接続してくる側 |
| クライアントプログラム | | | X Plugin 内部の「接続 (Client)」と同じ図に登場するときの、接続してくる側の呼び方 |
| サーバー | サーバー本体 | server | プロトコルの端点としての、接続を受ける側 (X Plugin を含む mysqld 全体) |
| 接続 | コネクション、クライアント (X Plugin 内部の要素を指すとき) | connection (X Plugin の実装上は `Client`) | 受け付けた TCP / Unix ソケット接続 1 本と、それに対応するサーバー側の要素 |
| セッション | | session (`Session`) | 認証を経て確立する、利用者としてコマンドを実行する文脈 |
| コネクションハンドラー | 接続層、コネクションハンドラ | connection handler | 接続の受付からセッションの確立・切断までを担う部分 |
| コマンドディスパッチャ | ディスパッチャ (初出時)、コマンドディスパッチャー | command dispatcher (`Dispatcher`) | セッションが受け取ったリクエストを振り分け、SQL 層に委ねて、レスポンスを返す部分 |
| 受け付ける / 受付 | accept する、アクセプト | accept | 接続を受け入れること。動詞は「受け付ける」、名詞は「受付」 |
| 切断 | 接続を閉じる (曖昧なとき) | | TCP / Unix ソケット接続を閉じること。セッションを閉じることには使わない |
| セッションを閉じる | セッションの切断 | `Session.Close` | 接続を維持したままセッションだけを終えること |
| kill | 強制終了 | kill (`kill_client`、`KILL` ステートメント) | 他の接続 (または自分自身) を強制的に閉じる操作 |
| acceptor スレッド | 受付スレッド、リスナースレッド | acceptor (スケジューラ名 `network`) | listen ソケットのイベントループを回すスレッド |
| ワーカースレッド | 作業スレッド | worker (スケジューラ名 `work`) | 接続 1 本の処理を担うスレッド |
| Expect スタック | | `Expectation_stack` | セッションごとに持つ、開いている Expect ブロックの入れ子 |
