## 資料

1. [WL#8338: X Plugin](https://dev.mysql.com/worklog/task/?id=8338)
   - X Plugin の設計文書
   - 接続受付 (33060、capability 交換、TLS)、SASL 認証 (チャレンジレスポンス、mysql.user との照合)、ワーカースレッド管理 (`mysqlx_min_worker_threads` / `mysqlx_max_connections` とステータス変数)、セッション管理 (Session.Reset、kill_client) が要件 (F-B1, F-C1...) として列挙されており、「コネクションハンドラーが何に責任を持つべきか」の資料

2. [MySQL Connection Handling and Scaling](https://dev.mysql.com/blog-archive/mysql-connection-handling-and-scaling/)
   - classic protocol 側だが、「接続受付 → user thread 生成 → クエリ実行 → 切断」という thread-per-connection モデルと接続スケーリングを公式が概説した記事で、コネクションハンドラー一般の責務のメンタルモデル作りに適していそう
   - X Plugin のワーカープール方式 (ngs::Scheduler_dynamic) との対比軸にもなりそう

3. [MySQL 8.4 Reference Manual: Connection Interfaces](https://dev.mysql.com/doc/refman/8.4/en/connection-interfaces.html)
   - サーバーがどう listen するか (TCP / Unix ソケット)、connection manager thread と thread cache の公式リファレンス

4. [MySQL 8.4 Reference Manual: X Plugin](https://dev.mysql.com/doc/refman/8.4/en/x-plugin.html)
   - 運用視点の章
   - 22.5.3 (TLS)、22.5.4 (caching_sha2_password との関係)、22.5.6 (接続・スレッド関連の変数一覧)、22.5.7 (Monitoring) が、責務の外形 (何が設定・観測可能か) を教えてくれる
   - なお [Implementation of the X Protocol by the X Plugin (doxygen)](https://dev.mysql.com/doc/dev/mysql-server/latest/mysqlx_protocol_xplugin.html) の「Implementation of the X Protocol by the X Plugin」も確認したが、中身は admin コマンド仕様 (kill_client / list_clients / ping) 中心で接続処理層の記述はほぼなく、今回の目的には薄い

読む順は 2 (一般モデル) → 1 (X Plugin の設計) → 4 (変数・監視) で、最終的な正はいつも通りローカルの `plugin/x/src` (Server::on_accept / Client::run / ngs::Scheduler_dynamic) が良さそう？
