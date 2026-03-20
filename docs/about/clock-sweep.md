# Clock sweep アルゴリズム

## 概要

- 限られた容量のキャッシュから、どの要素を追い出すかを決定するアルゴリズム
- 最近アクセスされた要素は追い出されにくく、しばらくアクセスされていない要素が優先的に追い出される
  - LRU アルゴリズムと似ているが、Clock sweep は参照ビットを用いて管理するため、アクセスのたびにリスト操作が必要な LRU よりも軽量な実装が可能 (Clock sweep は LRU を便利化したもの)

## 仕組み

- キャッシュ内の各スロットに対して「参照ビット」を管理する
- 時計の針のように巡回するポインタを持つ (0 → 1 → 2 → ... → 0)

### 要素アクセス時

- アクセスされたスロットの参照ビットを true にセットする

### 要素追い出し時

- ポインタが指しているスロットの参照ビットを確認する
  - true の場合: 参照ビットを false にクリアし、ポインタを次のスロットに進める
  - false の場合: そのスロットを追い出し対象として選択し、ポインタを次のスロットに進める
- この操作を追い出し対象が見つかるまで繰り返す

## 例

_3 スロットで、すべての要素が参照されている状態から追い出す場合_

```txt
初期状態: pointer=0
  slot 0: referenced=true
  slot 1: referenced=true
  slot 2: referenced=true

1 周目: すべて true なのでクリアしながら通過
  slot 0: true → false にクリア, pointer → 1
  slot 1: true → false にクリア, pointer → 2
  slot 2: true → false にクリア, pointer → 0

2 周目: クリア済みなので最初に見つかったスロットが追い出し対象
  slot 0: false → 追い出し対象, pointer → 1
```

- 全スロットが参照されている場合でも、最大 1 周のクリア走査で必ず追い出し対象が見つかる

_slot 1 のみ参照されていない場合_

```txt
初期状態: pointer=0
  slot 0: referenced=true
  slot 1: referenced=false
  slot 2: referenced=true

slot 0: true → false にクリア, pointer → 1
slot 1: false → 追い出し対象, pointer → 2
```

- 参照されていない要素が優先的に追い出される
