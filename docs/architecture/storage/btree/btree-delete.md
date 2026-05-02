# レコード削除

## 全体の流れ

1. ルートノードから再帰的にブランチノードを辿り、削除対象のリーフノードに到達する
2. リーフノードからレコードを削除する
   - 削除後にリーフノードがアンダーフローした場合、兄弟ノードからの転送またはマージを行う
3. リーフノードのマージによって親ブランチノードのレコード数が減り、親ブランチノードもアンダーフローした場合、同様に兄弟ブランチノードとの転送またはマージを行う (再帰的)
4. ルートノード (ブランチノード) のレコード数が 0 になった場合、唯一の子ノードを新しいルートに昇格させる

## リーフノードからの削除

リーフノードからの削除は以下の手順で行う

1. 二分探索により、削除すべきレコードを特定
2. レコードを削除
3. ノードの空き容量が閾値 (ノードの最大容量の半分) を下回る場合、以下のいずれかを行う
   - 兄弟ノードのレコードを自分のノードに移動
     - 基本的には右の (つまり自分より大きいキーを持つ) 兄弟ノードから転送が行われるが、右端のノードの場合は左の兄弟ノードから転送が行われる
   - 兄弟ノードとマージする
     - 兄弟のノードから転送が行われると、兄弟のノードの空き容量が閾値を下回る場合、兄弟ノードとマージする
4. ブランチノードのキーを更新する
   - 3 の処理でマージを行った場合はキーの更新が必要であるが、ノードが移動した場合も、兄弟との境界線が変わるため、ブランチノードのキーの更新が必要になる

### 例1. ノードの再分割が発生する場合

- 初期のB+Tree の構造:

    ```txt
                [Root: Branch Node]
                Records: [(key="fish", pageId=10), (key="monkey", pageId=20)]
                RightChild: pageId=30
                       /            |              \
                  (<fish)         (fish~monkey)    (>=monkey)
                  PageId=10       PageId=20       PageId=30
                    /                |                \
     [Leaf: apple,cat,dog]  [Leaf: fish,grape,lion]  [Leaf: monkey,tiger,zebra]
    ```

- 仮定
  - `key = "cat"` を削除する
  - 削除後にリーフノードの空き容量が閾値を下回る
- 流れ
  1. 二分探索により、削除すべきレコードを特定 (割愛)
  2. レコードを削除 (割愛)
  3. ノードの空き容量が閾値を下回るため、右の兄弟ノード (PageId=20) からレコードを移動
     - 右の兄弟ノード (PageId=20) から最小のレコード (key="fish") を移動
     - もともとは [apple, cat, dog] の順であったが、cat を削除して fish を移動したため、[apple, dog, fish] の順になる
     - 移動後のリーフノード (PageId=10): [apple, dog, fish]
     - 移動後の右の兄弟ノード (PageId=20): [grape, lion]
  4. ブランチノードのキーを更新
     - 移動前の境界線は "dog" と "fish" の間であったが、移動後の境界線は "fish" と "grape" の間になるため、ブランチノードのキーを "grape" に更新

### 例2. ノードのマージが発生する場合 (左端と真ん中のマージ)

- 初期のB+Tree の構造:

    ```txt
                [Root: Branch Node]
                Records: [(key="fish", pageId=10), (key="monkey", pageId=20)]
                RightChild: pageId=30
                       /            |              \
                  (<fish)         (fish~monkey)    (>=monkey)
                  PageId=10       PageId=20       PageId=30
                    /                |                \
     [Leaf: apple,cat]  [Leaf: fish,grape]  [Leaf: monkey,tiger,zebra]
    ```

- 仮定
  - `key = "cat"` を削除する
  - 削除後にリーフノードの空き容量が閾値を下回る
  - また右の兄弟ノード (PageId=20) からレコードを移動しても、右の兄弟ノードの空き容量が閾値を下回る

- 流れ
  1. 二分探索により、削除すべきレコードを特定 (割愛)
  2. レコードを削除 (割愛)
  3. ノードの空き容量が閾値を下回るため、右の兄弟ノード (PageId=20) からレコードを移動したいが、そうすると右の兄弟ノードの空き容量も閾値を下回るため、右の兄弟ノードとマージする
     - マージ後のリーフノード (PageId=10): [apple, fish, grape]
     - マージ後の右の兄弟ノード (PageId=20) は不要になるため、削除する
  4. ブランチノードのキーを更新
     - マージ前の境界線は "cat" と "fish" の間であったが、マージ後の境界線は "grape" と "monkey" の間になるため、ブランチノードのキーを "monkey" に更新
     - ブランチノードに紐づくリーフノードの数が K から K-1 になるため、ブランチノードのレコードも削除する

- 削除後のB+Tree の構造:

    ```txt
                [Root: Branch Node]
                Records: [(key="monkey", pageId=10)]
                RightChild: pageId=30
                       /            |
                  (<monkey)       (>=monkey)
                  PageId=10       PageId=30
                    /                |
     [Leaf: apple,fish,grape]  [Leaf: monkey,tiger,zebra]
    ```

### 例3. ノードのマージが発生する場合 (真ん中と右端のマージ)

- 初期のB+Tree の構造:

    ```txt
                [Root: Branch Node]
                Records: [(key="fish", pageId=10), (key="monkey", pageId=20)]
                RightChild: pageId=30
                       /            |              \
                  (<fish)         (fish~monkey)    (>=monkey)
                  PageId=10       PageId=20       PageId=30
                    /                |                \
     [Leaf: apple,cat]  [Leaf: fish,grape]  [Leaf: monkey,tiger]
    ```

- 仮定
  - `key = "monkey"` を削除する
  - 削除後にリーフノードの空き容量が閾値を下回る
  - また左の兄弟ノード (PageId=20) からレコードを移動しても、左の兄弟ノードの空き容量が閾値を下回る

- 流れ
  1. 二分探索により、削除すべきレコードを特定 (割愛)
  2. レコードを削除 (割愛)
  3. ノードの空き容量が閾値を下回るため、左の兄弟ノードからレコードを移動したいが、そうすると左の兄弟ノードの空き容量も閾値を下回るため、左の兄弟ノードとマージする
     - マージ後のリーフノード (PageId=20): [fish,grape,tiger]
     - 自分のノード（PageID=30）は空になり不要になるため、削除する
  4. ブランチノードのキーを更新
     - マージ前の境界線は "grape" と "monkey" の間であったが、マージ後の境界線は "cat" と "fish" の間になるため、ブランチノードのキーを "fish" に更新
     - ブランチノードに紐づくリーフノードの数が K から K-1 になるため、ブランチノードのレコードも削除する
     - また PageId=20 がRightChild に昇格する

- 削除後のB+Tree の構造:

    ```txt
                [Root: Branch Node]
                Records: [(key="fish", pageId=10)]
                RightChild: pageId=20
                       /            |
                  (<fish)       (>=fish)
                  PageId=10       PageId=20
                    /                |
     [Leaf: apple,cat]  [Leaf: fish,grape,tiger]
    ```

※ ポイントとして、マージの際は常に「左のノードに右のノードをマージする」というルールにしている (要するに、マージの際には常に左のノードが残る)\
このようにしているのは、「右のノードに左のノードをマージする」と比較して高速であるため\
(append だとシフト操作が不要なので prepend よりも高速であるため)

### 転送不可かつマージ不可のケース

- 通常、転送すると転送元の空き容量が閾値を下回ると (容量の半分以下になると) 転送不可と判断される
- またその場合は転送ではなく、兄弟ノードとのマージが選択される
- ただし以下のような場合では、転送不可と判断されても 2 つのノードの合計データ量が 1 ノードの容量を超えてしまい、マージできないケースがある
  - アンダーフローしたノード A の空き容量: `≒ capacity/2`
  - 兄弟ノード B の使用量: `≒ capacity/2 + a` (α はレコードのサイズ)
  - この場合、ノード B の レコード a を転送すると、転送後のノード B は閾値を下回る
  - ただしマージすると、マージ後のノードの容量は `capacity + a` となる
- このような場合は、マージを注視してアンダーフロー (一時的にノードの空き容量が閾値を下回ること) をそのまま許容する

## ブランチノードのアンダーフロー処理

- リーフノードの削除・マージによってブランチノードのレコード数が減り、ブランチノード自体がアンダーフローする (空き容量が閾値を超える) 場合がある
- その場合、リーフノードと同様に兄弟ブランチノードからの転送またはマージを行う
- ただし、リーフノードの転送とは異なり、ブランチノードの転送では親の境界キーを経由するローテーションが行われる
  - 転送時: 親の境界キーを子に下ろし、兄弟のキーを親に上げる
  - マージ時: 親の境界キーを下ろしてマージ先に追加し、その後にレコードを移動する

### 例1. ブランチノードのアンダーフロー (兄弟から転送するケース)

- 初期の B+Tree の構造:

    ```txt
                        [Root: Branch Node (PageID=100)]
                        Records: [(key=30, pageId=60)]
                        RightChild: pageId=70
                           /                    \
                      (<30)                   (>=30)
                    PageID=60                 PageID=70
                      /                          \
    [Branch (PageID=60)]                  [Branch (PageID=70)]
    Records: [(10,P1),(20,P2)]              Records: [(40,P4),(50,P5),(60,P6)]
    RightChild: P3                        RightChild: P7
       /      |      \                    /     |      |      \
      P1     P2      P3                 P4     P5     P6      P7
    ```

- 仮定
  - PageID=60 のブランチノードでレコード削除が起き、アンダーフローが発生した

- 流れ (右の兄弟 PageID=70 から転送)
  1. 親の境界キー (key=30) を自分 (PageID=60) の末尾に下ろす
     - 自分の RightChild (P3) を非キーフィールドとして `(30, P3)` を自分に挿入
     - 自分: Records=[(10,P1),(30,P3)], RightChild=P3 (まだ更新前)
  2. 兄弟 (PageID=70) の先頭レコード (40,P4) のキーを親に上げる
     - 親のキーを 30 → 40 に更新
  3. 兄弟の先頭レコードの非キーフィールド (P4) を自分の RightChild に設定
     - 自分: Records=[(10,P1),(30,P3)], RightChild=P4
  4. 兄弟の先頭レコード (40,P4) を削除
     - 兄弟: Records=[(50,P5),(60,P6)], RightChild=P7

- 転送後の B+Tree の構造:

    ```txt
                        [Root: Branch Node (PageID=100)]
                        Records: [(key=40, pageId=60)]
                        RightChild: pageId=70
                           /                    \
                      (<40)                   (>=40)
                    PageID=60                 PageID=70
                      /                          \
    [Branch (PageID=60)]                  [Branch (PageID=70)]
    Records: [(10,P1),(30,P3)]              Records: [(50,P5),(60,P6)]
    RightChild: P4                        RightChild: P7
       /      |      \                       /     |      \
      P1     P3      P4                    P5     P6      P7
    ```

### 例2. ブランチノードのアンダーフロー (マージするケース)

- 初期の B+Tree の構造:

    ```txt
                        [Root: Branch Node (PageID=100)]
                        Records: [(key=30, pageId=60)]
                        RightChild: pageId=70
                           /                    \
                      (<30)                   (>=30)
                    PageID=60                 PageID=70
                      /                          \
    [Branch (PageID=60)]                  [Branch (PageID=70)]
    Records: [(10,P1)]                      Records: [(40,P4)]
    RightChild: P3                        RightChild: P7
       /      \                              /      \
      P1      P3                           P4       P7
    ```

- 仮定
  - PageID=60 のブランチノードでレコード削除が起き、アンダーフローが発生
  - また、右の兄弟ノード (PageID=70) からレコードを移動しても、右の兄弟ノードの空き容量が閾値を下回る

- 流れ (右の兄弟 PageID=70 とマージ)
  1. 親の境界キー (key=30) を自分 (PageID=60) の末尾に下ろす
     - 自分の RightChild (P3) を非キーフィールドとして `(30, P3)` を自分に挿入
     - 自分: Records=[(30,P3)], RightChild=P3 (まだ更新前)
  2. 兄弟 (PageID=70) のレコードをすべて自分に移動
     - 自分: Records=[(30,P3),(40,P4)], RightChild=P3 (まだ更新前)
  3. 兄弟の RightChild (P7) を自分の RightChild に設定
     - 自分: Records=[(30,P3),(40,P4)], RightChild=P7
  4. 親から境界レコードを削除し、RightChild を自分 (PageID=60) に更新
     - 親: Records=[], RightChild=pageId=60

- マージ後の B+Tree の構造:

    ```txt
                [Root: Branch Node (PageID=100)]
                Records: []
                RightChild: pageId=60
                          |
                      (すべて)
                    PageID=60
                        |
                [Branch (PageID=60)]
                Records: [(30,P3),(40,P4)]
                RightChild: P7
                /      |      \
               P3     P4      P7
    ```

    ※ルートノードのレコードが空になった場合、ルートの子ノード (PageID=60) が新しいルートに昇格する
