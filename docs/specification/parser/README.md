# SQL パーサーの仕様

MySQL の SQL パーサーの仕様を以下の文書に記述している

1. [sql_parser.md](./sql_parser.md): SQL パーサーの論理モデル
   - 責務、構成要素、処理の流れ、MySQL の設計の要点、エラー
- 実装時の参照資料 (読む順には含めない): [reference/sql_parser_spec.md](./reference/sql_parser_spec.md)
  - SQL パーサーの詳細仕様 (構文の範囲の決め方、字句規則、演算子の優先順位、式とステートメントごとの規則、構文エラー)
  - 論理モデルと食い違う場合は論理モデルを正とし、そのドメインを実装する段階で更新する
