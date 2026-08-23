# file segment (inode)

## 概要

- file segment (以下「segment」) は、ページの専有単位。1 つの segment は「その segment に属する extent の集合」と「extent に属さない単ページ (frag ページ)」で構成される
- ページの利用者はすべて segment に属する。segment 外に残るのは、[FSP ヘッダー](fsp.md#fsp-ヘッダー)を置くページ (page 0)、[記述子ページ](fsp.md#extent-記述子)、および segment メタ情報 (inode) を格納する [inode ページ](#inode-ページ)のみで、これらはファイル共有の管理構造として segment 外に置かれる
- B+Tree は 2 つの segment (leaf segment と非リーフ segment) を持ち、リーフノードのページと非リーフノードのページを別 segment に分ける (詳細: [利用者ごとの segment 構成](#利用者ごとの-segment-構成))
- segment の管理主体は各 segment のメタ情報を保持する inode。inode への参照 (segment header) は、その segment を利用する側の固定アンカーページに置かれる (詳細: [segment header](#segment-header))

## inode ページ

- inode ページは、segment ごとの inode エントリを配列として格納するページ
- inode ページ自体は segment に属さず、[FSP ヘッダー](fsp.md#fsp-ヘッダー)の 2 リスト (SEG_INODES_FREE / SEG_INODES_FULL) で連結される
  - SEG_INODES_FREE: 未使用スロットが残っている inode ページのリスト
  - SEG_INODES_FULL: 全スロットが使用中の inode ページのリスト
- inode ページの先頭にはリスト連結用の node を置き、その後ろに inode エントリを配列で並べる
- 新しい segment を作るときに SEG_INODES_FREE が空なら、通常の[単ページ割り当て](fsp.md#ページ割り当て)で 1 ページを確保して inode ページに初期化し、SEG_INODES_FREE に追加する
- inode ページ内の全スロットが使用中になった時点で SEG_INODES_FREE から SEG_INODES_FULL へ移し、逆に全スロットが未使用に戻った時点で inode ページ自体を[単ページ解放](fsp.md#ページ解放)で返却する
- inode エントリの未使用判定は segment id が 0 かどうかで行う (segment id は 1 から採番するため、0 は未使用スロットの印として予約される)

## inode エントリ

inode エントリは 576 バイト。内訳は以下

| オフセット | バイト数 | 内容 |
| --- | --- | --- |
| 0 | 8 | segment id (0 = 未使用スロット) |
| 8 | 4 | NOT_FULL リストに属する extent の中で「segment 内で使用中と数えられているページ数」の総和 |
| 12 | 16 | FREE リスト (= この segment が専有していて全ページが空きの extent) の base node |
| 28 | 16 | NOT_FULL リスト (= この segment が専有していて一部ページが空きの extent) の base node |
| 44 | 16 | FULL リスト (= この segment が専有していて全ページが使用中の extent) の base node |
| 60 | 4 | 破損検出用のマジックナンバー |
| 64 | 512 | frag array (= 4 バイト × 128 slot。slot 未使用時は無効な PageNumber) |

- frag array は、segment が extent を専有していない段階で確保した単ページ (frag ページ) の PageNumber を並べたもの。slot 数は extent のページ数 (256) の半分 (128) で、[割り当て](#ページ割り当て)における frag ページ運用の上限と一致する
- 3 つの extent リスト (FREE / NOT_FULL / FULL) は、この segment に属する extent のみを連結する。ファイル全体の [FREE / FREE_FRAG / FULL_FRAG リスト](fsp.md#リスト構造) とは別のリストで、segment 内の状態遷移で独立に管理される
- NOT_FULL_N_USED は、NOT_FULL リストに属する extent のうち segment 側で「使用中」と数えているページ数の総和を保持する。割り当て・解放・extent のリスト間遷移に応じて増減し、segment の使用ページ数の算出に使う

## segment header

segment header は、利用側ページに置く inode への参照。以下の 6 バイト形式で表現する

| オフセット | バイト数 | 内容 |
| --- | --- | --- |
| 0 | 4 | inode エントリを含む inode ページの PageNumber |
| 4 | 2 | inode ページ内での inode エントリのオフセット (ボディ先頭からの相対値) |

- 参照先ファイルの識別子を持たないのは、segment とその利用者が常に同一ファイル内に閉じるため。ファイル内アドレスの表現形式は、リスト構造で使われる[アドレス](fsp.md#リスト構造)と同じ 6 バイト構成 (PageNumber 4 バイト + オフセット 2 バイト) をそのまま使う
- segment header の置き場所は利用者ごとに異なる (詳細: [利用者ごとの segment 構成](#利用者ごとの-segment-構成))
  - B+Tree: [メタページ](../btree/meta-page.md) に leaf segment / 非リーフ segment の 2 本を置く
  - Undo チェーン / DDL undo チェーン: 各チェーンの先頭ページ (= segment の最初のページ) に 1 本を置く
  - カタログヘッダー: [カタログのヘッダーページ](../dictionary/catalog.md#ヘッダーページ) 自身に 1 本を置く

## segment の作成

segment の新規作成は以下の順で行う

1. SEG_INODES_FREE リストから inode エントリを 1 つ確保する。リストが空なら inode ページを 1 枚新規確保して SEG_INODES_FREE に追加してからエントリを取る
2. [FSP ヘッダー](fsp.md#fsp-ヘッダー)の segment id カウンタから次の値を採番し、カウンタをインクリメントする
3. inode エントリを初期化する: segment id を書き込み、3 つの extent リスト (FREE / NOT_FULL / FULL) の base node を空で初期化し、frag array の全 slot を未使用値で埋め、マジックナンバーを設定する
4. 利用側から要求された場合は、この segment 自身から最初の 1 ページを[ページ割り当て](#ページ割り当て)で払い出し、そのページに segment header を書き込む

- 手順 1 で inode ページを新規確保したとき、確保したページをバッファプールへ作成できない場合は、そのページ割り当てを補償解放してから失敗を返す
- 手順 4 は、B+Tree の非リーフ segment とメタページの関係のように「segment の最初のページがそのまま segment header の置き場になる」利用者で使う
- segment header を別 segment のページに置く利用者 (= B+Tree の leaf segment。segment header は非リーフ segment の最初のページであるメタページに置かれる) では、手順 4 を省き、後から segment header だけを書き込む
- 手順 4 のページ割り当てが容量枯渇で失敗した場合は、手順 1 で確保した inode スロットを未使用に戻してから失敗を返す (segment id カウンタは戻さない)

## ページ割り当て

segment に対するページ割り当ては、以下の順で払い出しページを決める

1. segment の NOT_FULL リストの先頭 extent、なければ FREE リストの先頭 extent を選び、その extent の bitmap の最下位 free bit を割り当てる。割り当てによる extent のリスト間遷移 (FREE → NOT_FULL、NOT_FULL → FULL) を反映する
2. NOT_FULL / FREE のいずれも空で、使用ページ数 (= frag array の使用中 slot 数 + 専有 extent 中の使用中ページ数) が 128 未満なら、[単ページ割り当て](fsp.md#ページ割り当て)を空間側に依頼し、得られた PageNumber を frag array の空き slot に記録する
3. NOT_FULL / FREE のいずれも空で、使用ページ数が 128 以上なら、以下の順で extent を 1 つ獲得してから 1 に戻る
   1. 空間の FREE_FRAG リストの末尾 extent が lease 可能 (= 記述子ページを先頭に含み、予約分以外の全ページが空き) なら、その extent を XDES_FSEG_FRAG 状態にして segment の NOT_FULL リストの末尾に繋ぎ、予約分のページ数を NOT_FULL_N_USED に計上する
   2. lease できなければ、空間から新しい extent を確保して XDES_FSEG (= segment 専有) 状態にし、segment の FREE リストに繋ぐ。このとき segment が予約している総ページ数 (= frag ページ数 + FREE / NOT_FULL / FULL の全 extent のページ数) が 40 extent 相当以上なら、続けて extent を最大 4 つ先読み確保して FREE リストに繋ぐ (フリーリスト先読み)

- 保有 extent の空きページを最優先するのは、segment が既に予約した領域を使い切ってから空間の新規領域に手を伸ばすため
- フリーリスト先読みは補充の試行であり、途中で extent を確保できない場合はそこで打ち切る (先読みが失敗しても呼び出し元の割り当ては成功する)
- 割り当ての探索順 (NOT_FULL → FREE、最下位 free bit) は決定的で、呼び出し側からの位置ヒントを受け取らない。分割の局所性は「同じ segment に属する extent 内でページを確保する」という配置規則から得る
- 使用ページ数 128 の閾値は inode エントリの frag slot 数と同じ値で、「frag array が満杯になる直前まで単ページ割り当てで済ませ、以降は extent 専有に切り替える」比率に対応する
- 手順 2 で空間側から得られたページは、そのまま segment に属する frag ページ扱いになる。そのページを含む extent の状態 (FREE_FRAG / FULL_FRAG) は空間側の遷移規則に従う

## extent の状態拡張

segment 導入により、extent は空間側の 4 状態 (NOT_INITED / FREE / FREE_FRAG / FULL_FRAG) に加えて以下の 2 状態を取りうる。詳細な状態機械は [fsp.md](fsp.md#状態機械) を参照

| 状態 | 意味 |
| --- | --- |
| XDES_FSEG | ある segment が専有する extent。extent 記述子の segment id フィールドにその segment の id が入る |
| XDES_FSEG_FRAG | 記述子ページを先頭に含む extent を、単ページ用途で segment に貸し出している (lease している) 状態 |

- XDES_FSEG は空間の FREE リストから取り出された extent が segment に組み込まれた瞬間に遷移する。空間側のリストからは外れ、segment 側の 3 リスト (FREE / NOT_FULL / FULL) のいずれかに繋がる
- XDES_FSEG_FRAG は、記述子ページを含む extent のうち予約分 (= 先頭 1 ページ) 以外の全ページが空きに戻ったときに、segment への lease として扱えるようになる。lease された extent は segment の管理下で単ページ用途に使われ、通常の専有 extent と同じ NOT_FULL / FULL リストで扱う
- 記述子ページの予約ページ数は先頭 1 ページのみ。空間の予約ページマップは page 0 と記述子ページ以外を segment 側に開放する
- lease から返却するときは、予約分の使用中ページ数を残したまま状態を FREE_FRAG に戻し、空間の FREE_FRAG リストへ繋ぎ直す

## ページ解放

segment に属するページの解放は、以下の判定でルートが分かれる

- 解放対象が frag ページ (= inode エントリの frag array に登録されているページ) の場合:
  1. frag array 内の該当 slot を未使用値に戻す
  2. 空間の[単ページ解放](fsp.md#ページ解放)を呼び出し、bitmap の戻しと extent 状態の遷移を空間側に委ねる
- 解放対象が専有 extent のページの場合:
  1. extent の bitmap 上で該当ページの free bit を戻す
  2. extent の状態を FULL → NOT_FULL、あるいは NOT_FULL → 「実質全 free」に遷移させ、対応する inode 内のリスト間で移し替える
  3. NOT_FULL_N_USED を戻したページ数だけ減算する
  4. extent が実質全 free (= 予約分を除いた全ページが free) になった場合、segment の管理から外して空間の FREE リストへ返却する。lease された extent の場合は予約分の使用中ページ数を残したまま FREE_FRAG に戻す

- 二重解放の防止は空間側と同じく解放前の free bit 確認で行う (詳細: [fsp.md - 二重解放の防止](fsp.md#二重解放の防止))
- 解放対象ページの中身には書き込まない (詳細: [fsp.md - ページ解放](fsp.md#ページ解放))

### 事前取得フェーズと書き込みフェーズの分離

- 解放は「事前取得 (touch) フェーズ」と「書き込みフェーズ」の 2 つに分けて呼び出せる。touch フェーズは書き込みを開始する前に、状態遷移で必要になる全ページを取得して pin し、その後の書き込みフェーズに必要な遷移計画 (対象 xdes / 遷移先リスト / 予約分の扱いなど) を組み立てて返す。書き込みフェーズはこの計画に基づいて xdes / リスト / NOT_FULL_N_USED を更新する
- 2 フェーズに分けた API は、呼び出し側が「解放と別の書き込みを同一 mini-transaction 内で行うが、解放自身の書き込みは失敗しないことを保証したい」場合に使う。呼び出し側は touch フェーズを書き込み前に呼んで解放計画を確定させ、その後に自分の書き込みを行い、最後に書き込みフェーズを呼ぶ
- touch フェーズが返した計画が有効なのは、同一 mini-transaction 内で空間管理情報 (= FSP ヘッダー / xdes / inode) に介在する書き込みがない間に限る。B+Tree のマージのような「解放対象ページとは別の利用側ページ (= B+Tree ノード) しか書き換えない中間処理」を挟む場合は計画は無効化されない
- 途中で書き込みフェーズを呼ばずに mini-transaction を終える場合、pin と読みが増えるだけで空間管理情報は変更されない (= 解放は行われない)

## segment の解放

segment 全体の解放は step 型で行う。1 回の呼び出し (= 1 step) は以下のいずれか 1 単位のみを解放する

- extent 1 つを丸ごと解放する: segment の FULL リスト先頭、なければ NOT_FULL リスト先頭、なければ FREE リスト先頭の順で選び、その extent 全ページを free にして空間の FREE リストへ返却する。lease された extent の場合は予約分の使用中ページ数を残したまま FREE_FRAG に戻す
- frag ページ 1 枚を解放する: frag array の末尾 slot から順に取り出し、そのページを[単ページ解放](#ページ解放)する

3 つの extent リストと frag array のすべてが空になったら、inode エントリの segment id を 0 に戻して未使用スロット化する。inode ページの状態 (SEG_INODES_FREE / SEG_INODES_FULL 間の遷移、空になった場合のページ解放) を反映して完了

- 呼び出し側は完了マーカーを検出するまで step を繰り返す。1 step あたりの mini-transaction サイズが有界になるため、1 回の mini-transaction で数百ページを解放してログが肥大化することを防ぐ
- 冪等性: 解放が完了した segment への再呼び出しは、inode が既に解放済みであることの検出により、新たな解放を行わず完了を報告する。これによりリカバリ経由での再呼び出しが二重解放にならない
- 解放済みの検出は、再呼び出しが起こる文脈 (リカバリ中の DDL 巻き戻し) では並行して新しい segment が作られないことを前提とする。解放済みの inode スロットが別の segment に再利用された場合の同一性までは検出しない
- B+Tree の解放順序: leaf segment を最後まで step 解放してから、非リーフ segment を step 解放する。非リーフ segment の最後の 1 ページはメタページであるため、木全体の解放はメタページの解放で終わる

## 利用者ごとの segment 構成

### B+Tree

- 1 本の B+Tree は leaf segment と非リーフ segment の 2 segment で構成される
- ページ → segment の帰属はノード種別で決まる
  - リーフノードのページ → leaf segment
  - 分岐ノードのページ、およびメタページ → 非リーフ segment
- メタページは非リーフ segment の最初のページとして確保され、そこに 2 本の segment header (leaf / 非リーフ) が置かれる。木の生存中はメタページの PageNumber が変わらないため、segment header の位置も木の生存中不変となる
- ルートノードのページの帰属もノード種別で決まる (root の特別扱いはしない)。ルートが分割で新しい分岐ノードに差し替わる場合は非リーフ segment、collapse でリーフノードがルートに昇格する場合は leaf segment に属したまま扱う
- 木全体の解放は、leaf segment → 非リーフ segment の順で [segment の解放](#segment-の解放)を呼び、非リーフ segment の最後のページ (= メタページ) の解放で完了する

### Undo チェーン / DDL undo チェーン

- Undo チェーン全体、DDL undo チェーン全体はそれぞれ 1 つの segment に属する
- segment header は各チェーンの先頭ページ (= segment の最初のページ) に置く
- チェーンの追加ページはこの segment からの[ページ割り当て](#ページ割り当て)で払い出される

### カタログヘッダー

- カタログファイルのヘッダーページ (= [カタログのヘッダーページ](../dictionary/catalog.md#ヘッダーページ)) は、専用の 1 segment (以下「カタログヘッダー segment」) の最初のページとして確保される
- segment header はヘッダーページ自身に置く。segment は 1 ページ (= ヘッダーページ) のみで構成され、以降ページが追加されることはない
