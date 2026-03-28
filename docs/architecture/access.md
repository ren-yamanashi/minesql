# アクセスメソッド

## 参考文献

- [The physical structure of records in InnoDB](https://blog.jcole.us/2013/01/10/the-physical-structure-of-records-in-innodb/)

## 概要

- ディスク上のデータ構造を辿ってエグゼキュータの要求に応じたデータを返す
  - 実際のデータの読み書きはバッファプールに依頼する
  - つまりエグゼキュータとバッファプールの仲介を担う
- アクセスメソッドのデータ構造とアルゴリズムには [B+Tree](./storage/b+tree/b+tree.md) を採用

### エグゼキュータとの責務の境界

- アクセスメソッドは「論理的に存在するレコード」だけをエグゼキュータに返す責務を持つ
  - ソフトデリート (DeleteMark) や将来的な MVCC の可視性判定はアクセスメソッド層で処理する
  - エグゼキュータはこれらの物理的な実装詳細を意識する必要がない
- 具体的には、イテレータ (`ClusteredIndexIterator`, `SecondaryIndexIterator`) が DeleteMark = 1 のレコードをスキップし、エグゼキュータには有効なレコードだけが見える
- InnoDB でも同様に、MVCC の可視性判定はストレージエンジン層で行われ、上位層には見えるレコードだけが返される

## Record

エグゼキュータからアクセスメソッドに渡されるレコードの構造。設計の背景は [ADR 0005](../adr/0005.AccessMethodのRecord構造体の設計.md) を参照

### 構造

Record は Header / Key / NonKey の 3 つで構成される

_Header_

| フィールド | 型 | 説明 |
| --- | --- | --- |
| `DeleteMark` | `uint8` | 削除マーク (0: 有効, 1: 削除)。物理削除は後でパージによって行われる |

_Key_

| フィールド | 型 | 説明 |
| --- | --- | --- |
| `Key` | `[][]byte` | プライマリキーを構成するカラム値 |

_NonKey_

| フィールド | 型 | 説明 |
| --- | --- | --- |
| `LastModified` | `uint64` | 自分を最後に変更したトランザクション ID |
| `RollPtr` | `uint64` | 自分が更新される 1 世代前の行の内容 (undo ログ) へのポインタ (= ロールバックポインタ) |
| `NonKeyColumns` | `[][]byte` | プライマリキー以外のカラム値 |

- アクセスメソッドが Record を受け取り、B+Tree のレコードにエンコードする
- SELECT の戻り値には `DeleteMark` や `LastModified`、`RollPtr` は含まれない

### エンコード形式

B+Tree はレコード (フィールドの列) を格納し、キーフィールド数の指定に基づいてソート順を決定する (設計の背景は [ADR 0006](../adr/0006.セカンダリインデックスのソート順の実現方法.md) を参照)

#### クラスタ化インデックス

Record は B+Tree のレコードとして以下のようにエンコードされる

| フィールド | バイト数 | 領域 | 説明 |
| --- | --- | --- | --- |
| `DeleteMark` | 1 | ヘッダー | 削除マーク |
| Key | 可変 | キー | `Key` のカラム値を Memcomparable format でエンコードしたもの |
| `LastModified` | 8 | 非キー | 自分を最後に変更したトランザクション ID |
| `RollPtr` | 8 | 非キー | ロールバックポインタ |
| NonKeyColumns | 可変 | 非キー | `NonKeyColumns` のカラム値を Memcomparable format でエンコードしたもの |

#### セカンダリインデックス

Record は B+Tree のレコードとして以下のようにエンコードされる

| フィールド | バイト数 | 領域 | 説明 |
| --- | --- | --- | --- |
| `DeleteMark` | 1 | ヘッダー | 削除マーク |
| セカンダリキー | 可変 | キー | セカンダリインデックスを構成するカラム値を Memcomparable format でエンコードしたもの |
| プライマリキー | 可変 | キー | プライマリキーを構成するカラム値を Memcomparable format でエンコードしたもの |

- B+Tree 内のソート順は (セカンダリキー, プライマリキー) で決まる
- これにより、同じセカンダリキーで delete-marked と active のレコードが B+Tree 上で共存できる
- セカンダリインデックスには `LastModified` や `RollPtr` は格納されないため、非キー領域はない

## テーブル

- 1 つの `${tableName}.db` ファイル (= 1 テーブル) に対しては 1 つの `TableAccessMethod` が存在する
  - 1 つのテーブルには、1 つのクラスタ化インデックス (の B+Tree) と、複数のセカンダリインデックス (の B+Tree) が存在する可能性がある
- `TableAccessMethod` はテーブルへのアクセス手段を提供する (テーブル作成、行の挿入/削除/更新、など)

### クラスタ化インデックス

- プライマリキーはクラスタ化インデックスとする
- テーブルの実態は、プライマリキーをキーフィールド、その他のカラム値を非キーフィールドとするレコード
- プライマリキーが複合キーの場合も考慮して (複合キーの場合も正しくソートされるように) Memcomparable format を使用してプライマリキーをエンコードする ([参照](../about/memcomparable-format.md))
- エンコード形式の詳細は [Record のエンコード形式](#エンコード形式) を参照

### セカンダリインデックス

- セカンダリインデックスのキーフィールドにはセカンダリキーとプライマリキーの両方が含まれる
  - B+Tree のソート順は (セカンダリキー, プライマリキー) で決まる
  - ユニーク制約はアクセスメソッド層でセカンダリキー部分のみに対してチェックする (delete-marked レコードはスキップ)
- 検索の際は、まずセカンダリインデックスを検索し、(必要であれば) キーフィールドに含まれるプライマリキーを使って、実際のレコードをテーブルから取得する
- 現状はユニークインデックスのみサポート
  - 複合ユニークはサポートしてない
- エンコード形式の詳細は [Record のエンコード形式](#エンコード形式) を参照

## 行の削除

- 行の削除は DeleteMark を 1 にセットするインプレース更新 (soft delete) で実現する
  - 物理削除は後でパージによって行われる (パージは未実装)
- クラスタ化インデックス: レコード内の DeleteMark を 1 に更新
- セカンダリインデックス: レコード内の DeleteMark を 1 に更新
- イテレータは DeleteMark = 1 のレコードをスキップする

## 行の更新

- テーブル (クラスタ化インデックス) における行の更新は、キー (=プライマリキー) が変わるかどうかで処理が異なる
  - キーが変わらない場合は、インプレース更新を行う
  - キーが変わる場合は、soft delete + Insert で実現する
- セカンダリインデックスにおける行の更新は、常に soft delete + Insert で実現する (セカンダリインデックスはインプレース更新をサポートしない)
  - これは MySQL の InnoDB の方針に従っている (参照: [InnoDB のマルチバージョニング](https://dev.mysql.com/doc/refman/8.0/ja/innodb-multi-versioning.html) の「マルチバージョニングおよびセカンダリインデックス」の項目)

### セカンダリインデックスの更新の流れ

セカンダリインデックスのキーフィールドは (セカンダリキー, プライマリキー) で構成されるため、soft delete 後に同じセカンダリキーで新しいレコードを Insert しても B+Tree 上のキーは衝突しない

例: テーブル `users (id PK, name, email UNIQUE)` で `email = 'alice@example.com'` の行の `id` を `1` → `2` に更新する場合

```txt
1. soft delete: key=(alice@example.com, 1) の DeleteMark を 1 にセット
2. Insert:      key=(alice@example.com, 2) を DeleteMark=0 で挿入

B+Tree の状態:
  key=(alice@example.com, 1)  DeleteMark=1  ← soft deleted
  key=(alice@example.com, 2)  DeleteMark=0  ← active
```

ユニーク制約のチェックはセカンダリキー部分 (`alice@example.com`) のみに対して行い、DeleteMark=1 のレコードはスキップする
