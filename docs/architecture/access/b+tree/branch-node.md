# ブランチノード

- 構造

  ```txt
  |ノードタイプヘッダー |ブランチノードヘッダー|Slotted Page  |
  ↑_____8 bytes_____↑______8 bytes_____↑__4080 bytes__↑
  ```

  - 合計 4096 byte (1 ページのサイズ)
  - ノードタイプヘッダー: 8 byte (`"BRANCH  "`)
  - ブランチノードヘッダー: 8 byte (右子 PageId)
  - Slotted Page: 4080 byte

- ブランチノードのデータ構造は Slotted Page になっているので、データの挿入などは [Slotted Page](./slotted-page.md) の仕様に従う
- Slotted Page のスロット数 = key-value ペアの数
  - key: 子ノードを分割する境界値
  - value: key 未満の値を持つ子ノードのページ ID

## ブランチノードヘッダーの構成

- サイズ: 8 byte
  - 右の子ノードへのポインタ (PageId): 8 byte
    - ブランチノードの右の子ノードの PageId を格納する
    - PageId は FileId (4 byte) + PageNumber (4 byte) で構成される

### 右の子ノードの PageId を格納する理由

- 前提として、B+Tree のブランチノードでは、n 個のキーに対して n+1 個の子ノードのポインタが必要
- Slotted Page に n 個のペア (key, childPageId) を格納すると、n 個の子ポインタしか格納できない
  - そのため残りの 1 個 (右端の子ポインタ) をヘッダーに格納する
  - 補足: 教科書的な B+Tree の構造ではこのようにブランチノードは、key-value ペアの数 + 1 個の子ノードのポインタを持つことが多い
    - が、MySQL with InnoDB の実装ではブランチノードは key-value ペアの数と同数の子ノードのポインタを持っているっぽい
      - 参考:
        - https://planetscale.com/blog/btrees-and-database-indexes#the-btree
        - https://blog.jcole.us/2013/01/10/btree-index-structures-in-innodb/

_以下例_

```txt
ブランチノード:
┌─────────────────────────────────────┐
│ Header: RightChildPageId = 30       │
├─────────────────────────────────────┤
│ Pair 0: (key=50,  value=10)         │
│ Pair 1: (key=100, value=20)         │
└─────────────────────────────────────┘

木構造:
            [Branch: 50, 100]
            /       |         \
          10       20          30
        (N<50) (50<=N<100)   (100<=N)
        /           |              \
[Leaf:10,20,30] [Leaf:60,70] [Leaf:110,120]
(PageID=10)     (PageID=20)   (PageID=30)
```

## ブランチノードに key-value ペアを挿入

[リーフノードに key-value ペアを挿入](./leaf-node.md#リーフノードに-key-value-ペアを挿入) と同様の手順で挿入

## ブランチノード内の特定の key-value ペアの key を更新

- 子ノード側でペアの追加・削除が起きて境界値 (最小キー) が変わった場合に、ブランチノード側のキーも更新する必要がある
  - 例えば、ブランチノードのペア (key=50, value=10) に対応する子ノードの最小キーが 50 から 60 に変わった場合、ブランチノードのペアの key も 50 から 60 に更新する必要がある
- その際、ブランチノードのペアの value (子ノードのページ ID) は変わらないので、key だけを更新する

## 兄弟ノードにペアを転送する

[リーフノードの兄弟ノードにペアを転送する](./leaf-node.md#兄弟ノードにペアを転送する) と同様の手順で転送
