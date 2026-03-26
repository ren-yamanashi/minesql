# アクセスメソッド

## 参考文献

- [The physical structure of records in InnoDB](https://blog.jcole.us/2013/01/10/the-physical-structure-of-records-in-innodb/)

## 概要

- ディスク上のデータ構造を辿ってエグゼキュータの要求に応じたデータを返す
  - 実際のデータの読み書きはバッファプールに依頼する
  - つまりエグゼキュータとバッファプールの仲介を担う
- アクセスメソッドのデータ構造とアルゴリズムには [B+Tree](./storage/b+tree/b+tree.md) を採用

## Record

エグゼキュータからアクセスメソッドに渡されるレコードの構造。設計の背景は [ADR 0005](../adr/0005.AccessMethodのRecord構造体の設計.md) を参照

### 構造

Record は Header / Key / Value の 3 つで構成される

_Header_

| フィールド | 型 | 説明 |
| --- | --- | --- |
| `DeleteMark` | `uint8` | 削除マーク (0: 有効, 1: 削除)。物理削除は後でパージによって行われる |

_Key_

| フィールド | 型 | 説明 |
| --- | --- | --- |
| `Key` | `[][]byte` | プライマリキーを構成するカラム値 |

_Value_

| フィールド | 型 | 説明 |
| --- | --- | --- |
| `LastModified` | `uint64` | 自分を最後に変更したトランザクション ID |
| `RollPtr` | `uint64` | 自分が更新される 1 世代前の行の内容 (undo ログ) へのポインタ (= ロールバックポインタ) |
| `NonKeyFields` | `[][]byte` | プライマリキー以外のカラム値 |

- アクセスメソッドが Record を受け取り、B+Tree の [key-value ペア](./storage/b+tree/node/pair.md) にエンコードする
- SELECT の戻り値には `DeleteMark` や `LastModified`、`RollPtr` は含まれない

### エンコード形式

#### クラスタ化インデックス

Record は B+Tree の key-value ペアとして以下のようにエンコードされる

- key: `Key` のカラム値を Memcomparable format でエンコードしたもの
- value: 以下の内容を連結したもの

| フィールド | バイト数 | 説明 |
| --- | --- | --- |
| `DeleteMark` | 1 | 削除マーク |
| `LastModified` | 8 | 自分を最後に変更したトランザクション ID |
| `RollPtr` | 8 | ロールバックポインタ |
| NonKeyFields | 可変 | `NonKeyFields` のカラム値を Memcomparable format でエンコードしたもの |

#### セカンダリインデックス

- key: セカンダリインデックスを構成するカラム値を Memcomparable format でエンコードしたもの
- value: 以下の内容を連結したもの

| フィールド | バイト数 | 説明 |
| --- | --- | --- |
| `DeleteMark` | 1 | 削除マーク |
| プライマリキー | 可変 | プライマリキーをエンコードしたもの |

※ セカンダリインデックスには `LastModified` や `RollPtr` は格納されない

## テーブル

- 1 つの `${tableName}.db` ファイル (= 1 テーブル) に対しては 1 つの `TableAccessMethod` が存在する
  - 1 つのテーブルには、1 つのクラスタ化インデックス (の B+Tree) と、複数のセカンダリインデックス (の B+Tree) が存在する可能性がある
- `TableAccessMethod` はテーブルへのアクセス手段を提供する (テーブル作成、行の挿入/削除/更新、など)

### クラスタ化インデックス

- プライマリキーはクラスタ化インデックスとする
- テーブルの実態は、プライマリキーを `key` とし、その他のカラム値を結合したものを `value` とする
- プライマリキーが複合キーの場合も考慮して (複合キーの場合も正しくソートされるように) Memcomparable format を使用してプライマリキーをエンコードする ([参照](../about/memcomparable-format.md))
  - データを挿入する際は、key, value をそれぞれエンコードしてから保存 (B+Tree に挿入) する
- エンコード形式の詳細は [Record のエンコード形式](#エンコード形式) を参照

### セカンダリインデックス

- セカンダリインデックスには、セカンダリインデックスを構成するカラム値を key, プライマリキーを value として格納する
- 検索の際は、まずセカンダリインデックスを検索し、(必要であれば) その value であるプライマリキーを使って、実際のレコードをテーブルから取得する
- 現状はユニークインデックスのみサポート
  - 複合ユニークはサポートしてない
  - key = セカンダリインデックスのカラム値、value = プライマリキーのペアを Memcomparable format でエンコードしたものを使用
- エンコード形式の詳細は [Record のエンコード形式](#エンコード形式) を参照

## 行の更新

- テーブル (クラスタ化インデックス) における行の更新は、キー (=プライマリキー) が変わるかどうかで処理が異なる
  - キーが変わらない場合は、インプレース更新を行う
  - キーが変わる場合は、Delete + Insert で実現する
- セカンダリインデックスにおける行の更新は、常に Delete + Insert で実現する (セカンダリインデックスはインプレース更新をサポートしない)
  - これは MySQL の InnoDB の方針に従っている (参照: https://dev.mysql.com/doc/refman/8.0/ja/innodb-multi-versioning.html の「マルチバージョニングおよびセカンダリインデックス」の項目)
