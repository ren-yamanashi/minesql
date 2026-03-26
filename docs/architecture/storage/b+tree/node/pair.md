# key-value ペア

## 概要

- B+Tree のノードには、複数の key-value ペアが格納される
- key-value のペアはそれぞれエンコードされる
  - キーが複合キーの場合も考慮して (複合キーの場合も正しくソートされるように) Memcomparable format を使用してキーをエンコードしている ([参照](../about/memcomparable-format.md))

## シリアライズ形式

key-value ペアは以下の形式でバイト列にシリアライズされ、[Slotted Page](./slotted-page.md) に格納される

| フィールド | バイト数 | 説明 |
| --- | --- | --- |
| `key_size` | 4 | key 部分のバイト数 |
| `key` | 可変 | キーのバイト列 |
| `value` | 可変 | 値のバイト列 (全体のバイト数から `key_size` + `key` を引いた残り) |

## ブランチノード

- key: 子ノードを分割する境界値が格納される
- value: key 未満の値を持つ子ノードのページ ID が格納される

※この辺りは [ブランチノード](./branch-node.md) をみた方がわかりやすい

## リーフノード

- key と value に何を格納するかは、B+Tree を利用する側 ([アクセスメソッド](../../../access.md)) の責務としている
- B+Tree は key と value の中身については関与しない
