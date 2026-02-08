# パーサー

## 概要

- クエリを構文解析して抽象構文木の形にする
- Tokenize -> Parse のように 2 ステップで処理を行うのではなく、Tokenize と Parse を一列で処理する
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

## Tokenizer

