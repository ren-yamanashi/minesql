# パーサー

## 概要

- クエリを構文解析して抽象構文木の形にする
- Tokenize -> Parse のように 2 ステップで処理を行うのではなく、Tokenize と Parse を一列で処理する (https://github.com/fb55/htmlparser2 を参考)
- Tokenizer がトークンを識別したら、その都度 Parser にイベント通知
  - Tokenize でトークンの種別を識別した後に、その種別ごとに Parser にイベント通知する (e.g. SELECT キーワードなら `OnSelect` イベント)

以下ざっくりフロー

```mermaid
flowchart
    %% ノード定義
    Input[SQL]
    Tokenizer
    Parser
    AST

    %% フロー
    Input --> Tokenizer
    Tokenizer -- "1. トークン発見 (e.g. SELECT, users, ...)" --> Event((イベント通知))
    Event -- "2. OnKeyword / OnIdentifier..." --> Parser
    Parser -- "3. ASTを更新 / State遷移" --> AST
    AST -.->|"4. 次の文字へ"| Tokenizer
```

## 設計の補足

各ステートメント (e.g. SELECT, INSERT, CREATE TABLE) ごとの処理が割と複雑なので、ステートメントごとに StatementParser (e.g. SelectParser, InsertParser) を用意し、Parser が (ステートメントに応じて) それを呼び出す形にしている

以下ざっくりクラス図

```mermaid
classDiagram
    %% Tokenizer
    class Tokenizer {
        +Tokenize()
    }
    class TokenHandler {
        <<interface>>
        +onKeyword(word string)
        +onIdentifier(ident string)
        +onSymbol(symbol string)
        +onString(value string)
        +onNumber(num string)
        +onComment(text string)
        +onError(err error)
    }

    %% Parser
    class Parser {
        -stmtParser StatementParser
    }
    class StatementParser {
        <<interface>>
        TokenHandler
        +getResult() Statement
        +getError() error
        +finalize()
    }
    class SelectParser
    class InsertParser
    class DeleteParser
    class UpdateParser
    class TransactionParser

    %% CreateParser とサブパーサー
    class CreateParser {
        -colParser *ColumnDefParser
        -conParser *ConstraintDefParser
    }
    class ColumnDefParser {
        +onKeyword(word string)
        +finalize() error
        +getDef() Definition
    }
    class ConstraintDefParser {
        +onKeyword(word string)
        +onIdentifier(ident string)
        +onSymbol(symbol string)
        +finalize() error
        +getDef() Definition
    }

    %% WhereParser (SelectParser, DeleteParser, UpdateParser が利用)
    class WhereParser {
        +initWhere()
        +pushColumn(ident string)
        +pushLiteral(lit Literal)
        +handleOperator(op string)
        +finalizeWhere() *WhereClause
    }

    %% 関係性
    Tokenizer --> TokenHandler
    TokenHandler <|-- Parser
    Parser o-- StatementParser
    StatementParser <|.. SelectParser
    StatementParser <|.. InsertParser
    StatementParser <|.. DeleteParser
    StatementParser <|.. UpdateParser
    StatementParser <|.. TransactionParser
    StatementParser <|.. CreateParser
    CreateParser o-- ColumnDefParser
    CreateParser o-- ConstraintDefParser
    SelectParser o-- WhereParser
    DeleteParser o-- WhereParser
    UpdateParser o-- WhereParser
```
