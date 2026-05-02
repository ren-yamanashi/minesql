# B+Tree

## 概要

- 参考
  - [MySQL with InnoDB のインデックスの基礎知識とありがちな間違い](https://techlife.cookpad.com/entry/2017/04/18/092524)
  - [インデックスの役割と構成](https://qiita.com/immrshc/items/6cdb47f61e10de8ee1a5)
- B+Tree の[ノード](./node/node.md)はページ単位
- B+Tree 全体はあくまで構造 (抽象) にすぎず、B+Tree 内のノード (=ページ) がバッファプールに格納されているのか、ディスクに格納されているのかあという情報は関与しない
- B+Tree のリーフノードは双方向連結リストになっているため、全件スキャンや範囲検索などで、容易に (効率的に) リーフノードを走査することができる

## B+Tree の作成

- 新しい B+Tree を作成する際は、まず空の[リーフノード](./node/leaf-node.md)を 1 つ作成し、そのノードをルートノードとして設定する
- また[メタページ](./meta-page.md)も作成し、ルートノードのページ ID をメタページに保存する

## ノードの検索

1. 目的のリーフノードに到達するまで、二分探索で[ブランチノード](./node/branch-node.md)を辿る
2. リーフノード内を二分探索する

### 例

_key = "grape" を検索_

- B+Tree の構造:

    ```txt
                [Root: Branch Node]
                Records: [(key="fish", pageId=10), (key="monkey", pageId=20)]
                RightChild: pageId=30
                       /            |              \
                  (<fish)       (fish~monkey)    (>=monkey)
                  PageID=10       PageID=20       PageID=30
                    /                |                \
     [Leaf: apple,cat,dog]  [Leaf: fish,grape,human]  [Leaf: monkey,tiger,zebra]
    ```

1. ルートノード (ブランチノード) で二分探索
   - "grape" と "fish" を比較 → fish < grape
   - "grape" と "monkey" を比較 → grape < monkey
   - → インデックス 1 のレコード (key="monkey", pageId=20) を取得

2. 子ページ (PageID=20) を取得して探索

3. リーフノードで二分探索
   - "grape" と "fish" を比較 → fish < grape
   - "grape" と "grape" を比較 → 一致
   - → インデックス 1 のレコード (key="grape") を取得して返す

## レコード挿入

- [レコード挿入](./btree-insert.md) を参照

## レコード削除

- [レコード削除](./btree-delete.md) を参照

## 非キーの更新

- 参照
  - [リーフノードのレコードの非キーを更新](./node/leaf-node.md#リーフノード内の特定のレコードの非キーを更新)
