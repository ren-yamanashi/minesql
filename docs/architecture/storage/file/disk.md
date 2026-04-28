# ディスク

## 概要

- データのバッファリングは独自のバッファプールで行われるため、ディスクへの書き込みには OS のキャッシュを利用しない (O_DIRECT を使用する)
- [バッファプール](../buffer/buffer-pool.md)から依頼された通りに[ページ](../page/page.md)を永続化 (読み書き) する
  - [PageId](../page/page.md#pageid) を受け取り、ページ単位でデータを読み書きする
  - ページの中身 (ページ内にどのようなデータが格納されているのかどうか) という点は一切関知しない

## ファイル

- ヒープファイル構造
  - ヒープファイル: ページという固定の長さごとに区切ったファイル
- ヒープファイルは FileId で識別される
- テーブルごとに個別のヒープファイルを持つ
  - どのファイルがどの FileId に対応するかという情報は、[カタログ](../dictionary/catalog.md)が管理する
    - つまりファイル内に (ヘッダーなどに) FileId が格納されるわけではない
- ファイル内のページは PageId で識別される

## 操作

### PageId の採番

- 格納されるデータの単位 = 16KB (=16,384 バイト) になるため、ファイルサイズを 16,384 で割った値が次のページ番号になる
  - 例:
    - ファイルサイズが 0 バイトの場合、次のページ番号は 0
    - ファイルサイズが 16,384 バイトの場合、次のページ番号は 1
    - ファイルサイズが 32,768 バイトの場合、次のページ番号は 2

### ページの読み込み

- 指定された [PageId](../page/page.md#pageid) に対応するページからデータを読み込む
- PageId は PageNumber を持つため、その PageNumber をもとにファイルディスクリプタをページの先頭へ移動し、データを読み込む
  - 例: PageNumber が 2 の場合、ファイルディスクリプタを、ファイルの先頭位置から 32,768 バイト (2 * 16,384) へ移動してからデータを読み込む
  - 読み込むデータのサイズはページサイズ (16,384 バイト) となる

### ページの書き込み

- 指定された [PageId](../page/page.md#pageid) に対応するページにデータを書き込む
- 読み込みと同様に、PageId の PageNumber をもとにファイルディスクリプタをページの先頭へシークし、データを書き込む
  - 例: PageNumber が 2 の場合、ファイルディスクリプタを、ファイルの先頭位置から 32,768 バイト (2 * 16,384) へ移動してからデータを書き込む
  - 書き込むデータのサイズはページサイズ (16,384 バイト) となる

### ページの Sync

- 前述の通り、MineSQL では OS のキャッシュを使用せずに独自のバッファプールを使用しているため、ディスクへの書き込みには O_DIRECT を使用している
- そのため、基本的にディスクへの書き込みは、OS のキャッシュを経由せず直接ディスク (HDD/SSD) に書き込まれる
- ただし、ストレージ自体がデータをライトバックキャッシュに保持している可能性があり、それを考慮すると確実に書き込みを行うためには `fsync()` を呼び出す必要がある (と思われる)
  - 参考: https://lwn.net/Articles/457667/
  > I/O operations performed against files opened with O_DIRECT bypass the kernel's page cache, writing directly to the storage. Recall that the storage may itself store the data in a write-back cache, so fsync() is still required for files opened with O_DIRECT in order to save the data to stable storage. The O_DIRECT flag is only relevant for the system I/O API.
  - そのため、サーバーのプロセス停止時などには `Sync()` を呼び出す方針としている
