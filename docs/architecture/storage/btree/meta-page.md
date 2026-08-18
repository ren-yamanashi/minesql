# メタページ

## 概要

- B+Tree のメタデータを管理するためのページ
- 以下の情報を保持する

| フィールド      | オフセット | サイズ   | 説明 |
|---------------|----------|---------|------|
| rootPageId    | 0 - 7    | 8 バイト | ルートノードの PageId |
| leafPageCount | 8 - 15   | 8 バイト | リーフページの総数 |
| height        | 16 - 23  | 8 バイト | ツリーの高さ |
| leaf segment header | 24 - 29 | 6 バイト | leaf segment の inode への参照 (詳細: [file segment (inode) - segment header](../fsp/fseg.md#segment-header)) |
| 非リーフ segment header | 30 - 35 | 6 バイト | 非リーフ segment の inode への参照 (詳細: [file segment (inode) - segment header](../fsp/fseg.md#segment-header)) |

- メタページは非リーフ segment の最初のページとして確保され、木の生存中はその PageNumber が変わらない (詳細: [file segment (inode) - 利用者ごとの segment 構成](../fsp/fseg.md#利用者ごとの-segment-構成))

## 各フィールドの更新タイミング

### rootPageId

- ルートノードが分割されて新しいルートが作成されたとき、rootPageId を新しいルートの PageId に更新する
- ルートが縮退したとき (ブランチノードのレコード数が 0 になったとき)、rootPageId を唯一の子ノードの PageId に更新する

### leafPageCount

- リーフノードの分割が発生したときにインクリメントし、マージが発生したときにデクリメントする
- 初期値は 1 (ルートリーフノード 1 つ)

### height

- ルートノードが分割されて新しいブランチノードがルートになったときにインクリメントし、ルートが縮退したときにデクリメントする
- 初期値は 1 (ルートリーフノードのみ)

### leaf segment header / 非リーフ segment header

- B+Tree 作成時 (= 2 つの segment を作成した直後) に設定され、木の生存中は不変
