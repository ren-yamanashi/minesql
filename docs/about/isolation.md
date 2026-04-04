# Isolation を実現するためのアルゴリズム

## Conflict Serializability

- 競合する操作の処理順序に注目する考え方 (競合関係にある操作をどのような順番に処理するかの考え方)
- Conflict Serializability では、競合関係にある操作が、直列的 (逐次) に処理した場合のスケジューリングと同じになるように、処理の操作を決める
  - 逐次的に処理した場合のスケジューリングは、トランザクションの Isolation を保証するから、(競合する処理においては) それと同じ順序関係を保つスケジューリングであれば、同様にトランザクションの Isolation を保証できると考える
- "競合" の定義
  - 以下の条件を全て満たす場合、異なる二つのトランザクション ($T_1$, $T_2$) にそれぞれ属する操作の $op_i$ と $op_j$ は競合 (Conflict) していると考える
    - $T_1 \neq T_2$ (異なるトランザクションであある)
    - $op_i$ と $op_j$ が同じデータアイテムに対する操作である
    - $op_i$ と $op_j$ のうち少なくとも一方が書き込み操作である
- その上で、トランザクションに対してスケジュールが与えられると、Conflict する操作のペア群に順序関係が生まれる
- このとき、「逐次実行した場合の順序関係」と同じ順序関係 (Conflict Equivalent) が得られるスケジュールを Conflict Serializable なスケジュールと呼ぶ

### Conflict Serializable を実現するための Two-Phase Locking (2PL)

- Conflict Serializability は並列実行が許容されるか否かの判断軸
- その許容される並列実行をどのように実現するかのアルゴリズムとして Two-Phase Locking (2PL) がある
- (複数の対象に) ロックを取得する段階と、それらを解放する段階を分けるロック処理方式
- あるトランザクションを実行する際に、必要なデータアイテムに対して順にロックを獲得していき (成長フェーズ)、処理が終わったら順にロックを開放していく (縮小フェーズ)
- ロックを開放してから別のデータアイテムに対してロックを獲得することがないようにする
- これを守ることで、Conflict Serializable なスケジュールが得られる
- 2PL は整合性は保てるが、デッドロックが発生しやすいトレードオフがある

## Cascading Abort

- ある一つのトランザクションが異常終了 (Abort) した際、そのトランザクションが更新したデータを読み込んだ他のトランザクションも連鎖的に異常終了 (Abort) しなければならない現象
- 例
  - トランザクション $T_1$ がデータアイテム $X$ を更新し、その後トランザクション $T_2$ が $X$ を読み込んで処理を続けていたとする
  - もし $T_1$ が異常終了 (Abort) した場合、$T_2$ は $T_1$ が更新した $X$ を読み込んでいるため、$T_2$ も異常終了 (Abort) しなければならない
  - さらに、$T_2$ が更新したデータを読み込んで処理を続けていたトランザクション $T_3$ があった場合、$T_3$ も異常終了 (Abort) しなければならない
- 2PL では、ロックの獲得と解放の順序が決まっているため、Cascading Abort が発生しやすい

### Cascading Abort を防止するための Strict Two-Phase Locking

- Cascading Abort を防止するには、2PL に「ロックはトランザクション終了時まで開放しない」という制約を追加する
- つまり "徐々に" ロックを開放していくのではなく、すべてのロックをトランザクション終了時まで保持し、COMMIT または ROLLBACK (Abort) した後に一斉に開放する方式

## S2PL の課題

S2PL は Conflict Serializable なスケジュールを保証するが、以下の課題がある:

- Reader と Writer が互いにブロックする: あるトランザクションが行に書き込みロックを保持している間、別のトランザクションはその行を読み取ることすらできない。逆も同様
- デッドロック: 複数のトランザクションが互いにロックを待ち合う状態が発生しやすい
- スループットの低下: 読み取りが多いワークロードでは、ロック待ちが頻発しスループットが大きく低下する

特に「Reader が Writer をブロックする (またその逆も)」という特性は、読み取りが大半を占める一般的なワークロードにおいて深刻なボトルネックとなる。この課題を解決するのが MVCC

## MVCC (Multi-Version Concurrency Control)

参考:

- [InnoDB Multi-Versioning - MySQL 8.0 Reference Manual](https://dev.mysql.com/doc/refman/8.0/en/innodb-multi-versioning.html)
- [自作 DBMS に必要な体系的トランザクション知識 - zenn.dev](https://zenn.dev/primenumber/articles/dbms-isolation)

### 概要

- 同じデータ項目に対して複数のバージョンを保持する方式
- 読み取りは常にスナップショット (開始時点の一貫した状態) を参照するため、読み取りのためのロックが不要になる
- S2PL の「Reader と Writer が互いにブロックする」問題を解消する
- 書き込み同士の競合は引き続きロックまたは競合検出が必要

以下は MySQL/InnoDB の方式に基づいて記述

### 行のバージョン管理

InnoDB は各行に以下のフィールドを内部的に付与する

| フィールド | サイズ | 内容 |
| --- | --- | --- |
| DB_TRX_ID | 6 bytes | この行を最後に INSERT/UPDATE したトランザクションの ID |
| DB_ROLL_PTR | 7 bytes | Undo log record へのポインタ (ロールポインタ) |
| DB_ROW_ID | 6 bytes | 単調増加する行 ID (ユーザー定義の PK がない場合に使用) |

行の旧バージョンは Undo log に格納される。DB_ROLL_PTR がチェーンを形成し、更新履歴を遡ることができる:

```text
現在の行 (DB_TRX_ID=103, DB_ROLL_PTR→)
  → Undo log record (DB_TRX_ID=101, DB_ROLL_PTR→)
    → Undo log record (DB_TRX_ID=99, DB_ROLL_PTR=NULL)
```

### DML 操作時の動作

- INSERT: 新しい行を作成し、DB_TRX_ID に 自身のトランザクション ID を設定する。また Insert undo log を作成する
- UPDATE: Clustered Index 上の行を in-place で更新し、旧バージョンの内容を Update undo log に記録する。DB_ROLL_PTR を新しい undo log record に向ける
- DELETE: 行を物理削除せず delete-mark を設定する。物理削除は後の Purge 処理で行われる

### Read View による可視性の判定

各トランザクションは開始時に Read View を作成する。Read View は以下の情報を持つ

- 現在実行中のトランザクション ID のリスト
- 最小のアクティブなトランザクション ID
- 次に採番されるトランザクション ID

データスキャン時、各行の DB_TRX_ID を Read View と照合し、可視性を判定する

- DB_TRX_ID が Read View 作成時点で Commit 済み → 可視
- DB_TRX_ID が Read View 作成時点で未完了 (実行中) → 不可視 → DB_ROLL_PTR を辿って旧バージョンを探す
- DB_TRX_ID が Read View 作成後に採番された → 不可視 → 同様に旧バージョンを探す

不可視と判定された場合は DB_ROLL_PTR で Undo log を辿り、可視なバージョンが見つかるまで遡る。

※ Read View の作成タイミングは分離レベルによって異なる。REPEATABLE READ ではトランザクション内の最初の読み取り時に作成し以降使い回すが、READ COMMITTED では文ごとに作り直す。そのため READ COMMITTED では同一トランザクション内でも他のトランザクションが Commit した挿入行が見え、Phantom Read (P3) が発生する。

### Undo log の Purge

Undo log は 2 種類ある

- Insert undo log: ロールバックにのみ必要。トランザクション Commit 後に即座に破棄可能 (INSERT 時に書き込まれる)
- Update undo log: 一貫性読み取り (Consistent Read) で旧バージョンの再構築に使われる。そのため、この undo log を参照し得るトランザクションがすべて完了するまで破棄できない (UPDATE, DELETE 時に書き込まれる)

Purge スレッドが、不要になった Update undo log と delete-mark された行の物理削除を行う。長時間実行のトランザクションがあると、その間の undo log が蓄積し Undo tablespace が肥大化する

## Snapshot Isolation

### 概要

- MVCC によって実現される分離レベル
- 各トランザクションは開始時点で Commit 済みのデータのスナップショットを保持し、以降の読み取りでは常にそのスナップショットを参照する
- 読み取りにロックが不要なため、S2PL と比較して読み取り主体のワークロードで大幅にスループットが向上する

### ANSI SQL-92 の分離レベルとの関係

ANSI SQL-92 では以下の異常 (Anomaly) と分離レベルを定義している

| 異常 | 説明 |
| --- | --- |
| Dirty Read (P1) | 他のトランザクションの未 Commit の変更が見える |
| Non-Repeatable Read (P2) | 同じ行を 2 回読み取ると結果が異なる |
| Phantom Read (P3) | 範囲読み取りで、他のトランザクションが挿入した行が現れる |

| 分離レベル | P1 | P2 | P3 |
| --- | --- | --- | --- |
| Read Uncommitted | 発生する | 発生する | 発生する |
| Read Committed | 防止 | 発生する | 発生する |
| Repeatable Read | 防止 | 防止 | 発生する |
| Serializable | 防止 | 防止 | 防止 |

Snapshot Isolation は P1, P2, P3 のいずれも防止するため、ANSI の定義上は Serializable に見える。しかし実際には ANSI で定義されていない Write Skew という異常が発生し得るため、真の Serializable ではない。

Snapshot Isolation は Repeatable Read と Serializable の間に位置する分離レベルと言える。

### Write Skew

- Snapshot Isolation 固有の異常で、S2PL では発生しない
- 2 つのトランザクションがそれぞれ異なる行を読み取り、その読み取り結果に基づいてそれぞれ別の行を更新する場合に発生する
- 各トランザクションは自身のスナップショットを見ているため、相手の更新に気づけない
- 例: 病院の当直シフトで「最低 1 人は当直」という制約がある場合
  - $T_1$: 当直者が 2 人いることを確認 → 自分の当直を外す
  - $T_2$: 当直者が 2 人いることを確認 → 自分の当直を外す
  - 結果: 当直者が 0 人になり制約違反 (各トランザクションは相手の更新を見ていない)

### Serializable Snapshot Isolation (SSI)

- Write Skew を防止し、Snapshot Isolation を真の Serializable に拡張したアルゴリズム
- PostgreSQL が Serializable 分離レベルの実装として採用している
- MVCC の利点 (読み取りにロック不要) を維持しつつ、Serializable を保証する
