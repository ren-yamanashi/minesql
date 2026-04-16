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
