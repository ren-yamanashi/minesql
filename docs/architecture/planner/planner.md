# プランナー

## 参考文献

- Database Design and Implementation - Chapter 10: Query Planning
- [MySQL 8.0 Reference Manual - The Optimizer Cost Model](https://dev.mysql.com/doc/refman/9.6/en/cost-model.html)
- [MySQLのコスト見積もりを調整する](https://gihyo.jp/dev/serial/01/mysql-road-construction-news/0108)

## 概要

- AST を元に実行計画を作る
  - 実行計画に従ってエグゼキュータを作成し、実行する
- minesql ではコストベースの最適化を行い、最もコストの低い実行計画を選択する
- プランナーの責務は以下
  - 与えられた SQL 文が実際に意味を成すものであるかどうかを判断する
    - テーブルとフィールドが、カタログ内に実際に存在するか確認する
    - 指定したフィールドが、テーブルのスキーマに合っているか確認する
    - フィールドに対する操作が、型として正しいか確認する
  - 実行計画の構築
    - 多くのケースでは、1 つのクエリに対して、複数種類の実行計画が考えられる
    - そのため、考えられる実行計画ごとに、クエリコストを計算し、最もコストの低い実行計画を選択する
      - [コストモデル](cost.md) を用いて、実行計画のコストを算出し、最もコストの低い実行計画を選択する
    - 実行計画のそれぞれのノードはほとんどの場合[エグゼキュータのノード](../executor/executor.md#エグゼキュータのツリー構造)に対応している

## JOIN

- JOIN のアルゴリズムには Nested Loop Join (NLJ) を採用している
  - Block Nested Loop Join (BNLJ) や Batched Key Access Join (BKAJ) はサポートしていない
    - BNLJ や BKAJ の MySQL のドキュメント: https://dev.mysql.com/doc/refman/8.0/ja/bnl-bka-optimization.html
- 現状は INNER JOIN のみサポート

### 結合順序の最適化

- N 個のテーブルの結合順序は N! 通りあり、全探索は現実的ではない
- 貪欲法により最小コストの結合順序を探す
  - 全テーブルを候補として開始し、1 テーブルずつ結果セットに追加していく
  - 各ステップで残りの候補から、追加後の累積コストが最小になるテーブルを選ぶ
- 候補の選択時には以下を検証する
  - 初回: 全テーブルが駆動表候補
    - INNER JOIN は可換なので、FROM テーブルに限らずどのテーブルも駆動表になれる
    - WHERE 条件のうち候補テーブルのカラムのみを参照する条件があれば、駆動表側に分離してアクセスパスを最適化する ([WHERE 条件の分離](#where-条件の分離))
  - 2 回目以降: 候補テーブルと結果セット内のいずれかのテーブルを結ぶ ON 条件が存在するか確認。存在しない候補はスキップ

#### 例

```sql
SELECT * FROM users
INNER JOIN orders ON users.id = orders.user_id
INNER JOIN items ON orders.item_id = items.id;
```

仮に users が 10000 行、orders が 100 行、items が 5000 行の場合:

1. 初回: 全テーブルが駆動表候補。各テーブルのフルスキャンコストを比較し、最小の orders (100 行) を選択
2. 2 回目: 残りは users と items。orders との ON 条件がある users が候補。users の結合カラム (id) は PK なので eq_ref → 低コスト。users を選択
3. 3 回目: 残りは items。users との ON 条件がある items が候補 (orders.item_id = items.id だが、orders は既に結果セットに含まれるため items も候補になる)。items を選択

結果の結合順序: orders → users → items

### 内部表のアクセス方法

- ON 条件の結合カラムのインデックス有無で判別する
  - eq_ref
    - 結合カラムが単一カラム PK または UNIQUE INDEX の場合
    - 1 回のキー検索で 1 行を取得する
  - ref
    - 結合カラムに非ユニークインデックスがある場合
    - 1 回のキー検索で 1 キーあたりの平均マッチ行数を取得する
  - フルスキャン
    - 結合カラムにインデックスがない場合
    - 駆動表の各行に対して内部表を全走査する

### WHERE 条件の分離

- JOIN の WHERE 条件は、特定テーブルのカラムのみを参照する条件と、複数テーブルにまたがる条件に分離される
  - 例: `WHERE users.id = '1' AND orders.item = 'apple'` → `users.id = '1'` は users のみ、`orders.item = 'apple'` は orders のみを参照するため、それぞれのテーブルに分離できる
- 駆動表のカラムのみを参照する条件は、駆動表のアクセスパス選択に使われる (PK/UNIQUE INDEX の等値検索やレンジスキャンなど)
  - 例: `users.id = '1'` が駆動表 users に分離された場合、フルスキャンではなく PK の等値検索で 1 行のみ読む
- 分離できなかった条件 (複数テーブルにまたがる条件) は、結合後に Filter として適用される
  - 例: `WHERE users.name = orders.item` は users と orders の両方を参照するため分離できず、結合後に評価する
