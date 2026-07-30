# ディスク

## 概要

- データのバッファリングは独自のバッファプールで行われるため、ディスクへの書き込みには OS のキャッシュを利用しない (O_DIRECT を使用する)
- [バッファプール](../buffer/buffer-pool.md)から依頼された通りに[ページ](../page/page.md)を永続化 (読み書き) する
  - [PageId](../page/page.md#pageid) を受け取り、ページ単位でデータを読み書きする
  - ページの中身 (ページ内にどのようなデータが格納されているのかどうか) という点は一切関知しない

## ファイル

- ヒープファイル構造
  - ヒープファイル: ページという固定の長さごとに区切ったファイル
  - 先頭ページ (page 0) は FSP ヘッダーと extent 記述子配列を格納する (詳細: [ファイル空間管理](../fsp/fsp.md))
- ヒープファイルは FileId で識別される
- テーブルごとに個別のヒープファイルを持つ
  - どのファイルがどの FileId に対応するかという情報は、[カタログ](../dictionary/catalog.md)が管理する
  - あわせてファイル自身も、page 0 の FSP ヘッダーに自らの FileId を格納する (詳細: [ファイル空間管理 - FSP ヘッダー](../fsp/fsp.md#fsp-ヘッダー))
- ファイル内のページは PageId で識別される

## 操作

### PageId の採番

- ディスク層はページの採番を行わない
- 新しいページの PageNumber は[ファイル空間管理](../fsp/fsp.md#ページ割り当て)の割り当てで決まり、割り当ての状態はファイル先頭 (page 0) の FSP ヘッダーと extent 記述子が保持する。ファイルサイズからの採番導出は行わない

### ページの読み込み

- 指定された [PageId](../page/page.md#pageid) に対応するページからデータを読み込む
- PageId は PageNumber を持つため、その PageNumber をもとにファイルディスクリプタをページの先頭へ移動し、データを読み込む
  - 例: PageNumber が 2 の場合、ファイルディスクリプタを、ファイルの先頭位置から 8,192 バイト (2 * 4,096) へ移動してからデータを読み込む
  - 読み込むデータのサイズはページサイズ (4,096 バイト) となる

### ページの書き込み

- 指定された [PageId](../page/page.md#pageid) に対応するページにデータを書き込む
- 読み込みと同様に、PageId の PageNumber をもとにファイルディスクリプタをページの先頭へシークし、データを書き込む
  - 例: PageNumber が 2 の場合、ファイルディスクリプタを、ファイルの先頭位置から 8,192 バイト (2 * 4,096) へ移動してからデータを書き込む
  - 書き込むデータのサイズはページサイズ (4,096 バイト) となる

### ページの Sync

- 前述の通り、MineSQL では OS のキャッシュを使用せずに独自のバッファプールを使用しているため、ディスクへの書き込みには O_DIRECT を使用している
- そのため、基本的にディスクへの書き込みは、OS のキャッシュを経由せず直接ディスク (HDD/SSD) に書き込まれる
- ただし、ストレージ自体がデータをライトバックキャッシュに保持している可能性があり、それを考慮すると確実に書き込みを行うためには `fsync()` を呼び出す必要がある (と思われる)
  - 参考: https://lwn.net/Articles/457667/
  > I/O operations performed against files opened with O_DIRECT bypass the kernel's page cache, writing directly to the storage. Recall that the storage may itself store the data in a write-back cache, so fsync() is still required for files opened with O_DIRECT in order to save the data to stable storage. The O_DIRECT flag is only relevant for the system I/O API.
  - そのため、サーバーのプロセス停止時などには `Sync()` を呼び出す方針としている

### ページの解放

- ファイル内のページを「解放済み」としてマークし、再利用可能にするための経路
- 解放の流れは[ファイル空間管理](../fsp/fsp.md#ページ解放)が担う。解放済みであることは extent 記述子側が保持し、解放対象ページの中身には書き込まない
- この経路は [DDL](../access/ddl.md) のロールバック (B+Tree 解放、DDL Undo 専用領域のクリア) に加え、DELETE を起点とする B+Tree のノード統合や木の高さの縮小でも呼び出される (詳細: [レコード削除](../btree/btree-delete.md))
- 解放されたページは、後続のページ割り当てで再利用される (詳細: [ファイル空間管理 - ページ割り当て](../fsp/fsp.md#ページ割り当て))

### ファイルの削除

- 指定された FileId に対応するヒープファイルを物理的に削除する
- この経路は [DDL](../access/ddl.md) のロールバックでのみ呼び出される。通常のテーブル操作 (INSERT / UPDATE / DELETE) はファイル削除を起こさない
- 削除に先立って、バッファプール内に該当 FileId のキャッシュページが残っている場合はそれらを無効化 (=ダーティーであっても書き戻さず破棄) する。残ったまま削除すると、後続の LRU 追い出しが削除済みのファイルディスクリプタに書き戻そうとして整合性が壊れるため
- 削除されたファイルが使用していた FileId は再利用しない (詳細: [カタログ - ヘッダーページ](../dictionary/catalog.md#ヘッダーページ))
