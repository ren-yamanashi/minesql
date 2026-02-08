# パーサー

## 概要

- クエリを構文解析して抽象構文木の形にする
- Tokenize -> Parse のように 2 ステップで処理を行うのではなく、Tokenize と Parse を一列で処理する (https://github.com/fb55/htmlparser2 を参考)
- Tokenizer がトークンを識別したら、その都度 Parser にイベント通知
  - Tokenize でトークンの種別を識別した後に、その種別ごとに Parser にイベント通知する (e.g. SELECT キーワードなら `OnSelect` イベント)

以下ざっくりフロー

```mermaid
flowchart LR
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
      handler Handler (call handler methods)
    }
    %% Parser
    class Handler {
        <<interface>>
        +OnKeyword(keyword string)
        +OnIdentifier(name string)
        +OnSymbol(symbol string)
        +OnStringLiteral(value string)
        +OnNumberLiteral(value string)
    }
    class Parser {
      ...
      +currentStatementParser StatementParser
    }
    class StatementParser {
        <<interface>>
        +OnKeyword(keyword string)
        +OnIdentifier(name string)
        +OnSymbol(symbol string)
        +OnStringLiteral(value string)
        +OnNumberLiteral(value string)
    }
    class SelectParser {
      ...
    }
    class InsertParser {
      ...
    }
    class CreateTableParser {
      ...
    }

    %% 関係性
    Tokenizer --> Handler
    Handler <|-- Parser
    Parser o-- StatementParser
    StatementParser <|-- SelectParser
    StatementParser <|-- InsertParser
    StatementParser <|-- CreateTableParser
```
