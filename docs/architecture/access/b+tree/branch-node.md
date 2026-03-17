# ブランチノード

- 構造

  ```txt
  |ノードタイプ |ブランチノードヘッダー|Slotted Page  |
  ↑__8 bytes__↑______8 bytes_____↑__4080 bytes__↑
  ```

  - 合計 4096 byte (1 ページのサイズ)
  - ノードタイプ: 8 byte (`"BRANCH  "`)
  - ブランチノードヘッダー: 8 byte (右子 PageId)
  - Slotted Page: 4080 byte

- ブランチノードのデータ構造は Slotted Page になっているので、データの挿入などは [Slotted Page](./slotted-page.md) の仕様に従う
- Slotted Page のスロット数 = key-value ペアの数
  - key: 子ノードを分割する境界値
  - value: key 未満の値を持つ子ノードのページ ID

## ブランチノードヘッダーの構成

- サイズ: 8 byte
  - 右の子ノードへのポインタ (PageId): 8 byte (ノードタイプの次の 8 byte)
    - ブランチノードの右の子ノードの PageId を格納する
    - PageId は FileId (4 byte) + PageNumber (4 byte) で構成される

### 右の子ノードの PageId を格納する理由

- 前提として、B+Tree のブランチノードでは、n 個のキーに対して n+1 個の子ノードのポインタが必要
- Slotted Page に n 個のペア (key, childPageId) を格納すると、n 個の子ポインタしか格納できない
  - そのため残りの 1 個 (右端の子ポインタ) をヘッダーに格納する

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

## ブランチノードの分割

[リーフノードの分割](./leaf_node.md#リーフノードの分割) と同様の手順で分割
