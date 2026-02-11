# コーディング規則

## コードのコメント規則

- 文体: 敬体ではなく常体を使用する
  - ❌ 「エンコードします」
  - ✅ 「エンコードする」
- 半角スペース: 半角英数字と全角文字の間には半角スペースを開ける
  - ❌ 「8バイトのデータ」「memcmp可能」
  - ✅ 「8 バイトのデータ」「memcmp 可能」
- 冗長性の排除: 関数名の繰り返しを避け、簡潔に記述する
  - ❌ 「Encode はバイト列を memcmp 可能な形式にエンコードします」
  - ✅ 「バイト列を memcmp 可能な形式にエンコードする」
- 括弧: 全角括弧ではなく半角括弧を使用する
  - ❌ 「データ（複数ブロック）」
  - ✅ 「データ (複数ブロック)」
- コロン: 全角コロンではなく半角コロンを使用する
  - ❌ 「特性：」
  - ✅ 「特性:」

## テストコード規則

- テスト構造: GIVEN, WHEN, THEN 形式で記述する
  - GIVEN: テストの前提条件を設定
  - WHEN: テスト対象の操作を実行
  - THEN: 期待される結果を検証
  - GIVEN -> WHEN -> THEN の順序を厳守する
  - WHEN -> THEN -> WHEN -> THEN のような構造は避ける
  - 観点が異なる場合は、複数のテストケースに分割する
- t.Run の使用: すべてのテストケースは `t.Run` を使用して記述する
  - テストケース名は日本語で記述する
  - テーブル駆動テストの場合は、各ケースに `name` フィールドを追加する

## ファイル操作規則

- **重要**: `/tmp` ディレクトリは絶対に使用しない
  - ビルド出力、テスト用の一時ファイル、その他すべての用途で `/tmp` ディレクトリの使用を禁止する
  - 代わりに `.claude/.sandbox/` ディレクトリにファイルを作成し、そのファイルを使用する

## 設計メモ規則

- ユーザーから設計に関する指示を受けたら、`.claude/.ai-output/memo.md` に記録する
  - 指示の内容、理由、実装方針を明確に記述する
  - 後で設計の意図を振り返れるようにする

## 動作確認手順

コードを変更した後は、必ず以下の手順で動作確認を行う:

### 1. テストの実行

変更したパッケージのテストを実行する:

```bash
# 例: btree パッケージのテスト
cd internal/storage/access/btree
go test -v

# 例: bufferpool パッケージのテスト
cd internal/storage/bufferpool
go test -v
```

全体のテストを実行する:

```bash
# プロジェクトルートから全体のテストを実行
make test
```

### 2. examples の実行

プロジェクトルートから examples を実行する (相対パスを変更しないこと):

```bash
# btree の examples
go run examples/btree/insert/main.go
go run examples/btree/scan/main.go
go run examples/btree/search_key/main.go

# executor の examples
go run examples/executor/*.go

# planner の examples
go run examples/planner/main.go
```

**注意**: examples は必ずプロジェクトルートから実行すること。examples 内の相対パス (例: `examples/btree/data`) はプロジェクトルートからの実行を前提としている。
