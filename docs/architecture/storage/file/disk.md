# ディスク

## 概要

- RDBMS はデータを永続化するために、データをファイルに書き込む必要がある
- ディスクはファイルへの読み書きを担当する
- 読み書きするファイルはヒープファイル構造
  - ヒープファイル: ファイルを[ページ](../page/page.md)という固定の長さごとに区切ったファイル
  - I/O 操作はページ単位 (最小の I/O 単位はページ)
  - 読み込み元、書き込み先のページは [PageId](../page/page.md#pageid) で指定
- テーブルごとに個別のディスクファイルを持つ
  - ファイル名は `{table_name}.db`
  - 各テーブルは FileId を持ち、対応するディスクファイルに格納される
    - どのファイル (`${table_name}.db`) がどの FileId に対応するかという情報は、[カタログ](../dictionary/catalog.md) が管理する
    - つまりファイル内に FileId (ないしは PageId) が格納されるわけではない
  - テーブル本体とそのインデックスは同じディスクファイル (同じ FileId) を共有する
    - これは MySQL の設計 (File-Per-Table の場合) に従っている
    - 参考: [17.6.3.2 File-Per-Table Tablespaces](https://dev.mysql.com/doc/refman/8.4/en/innodb-file-per-table-tablespaces.html)  
      > A file-per-table tablespace contains data and indexes for a single InnoDB table, and is stored on the file system in a single data file.

- OS のキャッシュではなく、独自のバッファプールによりデータバッファリングを行うため、ディスクへの書き込みには O_DIRECT を使用する
  - そのためのライブラリとして [github.com/ncw/directio](https://github.com/ncw/directio) を使用している

## ディスクの責務

ディスクは PageId を使用してページを特定し、4096 バイト単位のデータを読み書きする。\
ページの中身が何であるか (ページ内にどのようなデータが格納されているのかどうか) という点は一切関知しない (ページデータの中身に意味を持たせるのはディスクよりも上のレイヤーの責任)

## 最小の I/O 単位をページにする理由

- ファイルはファイルシステムによって HDD や SDD などのデバイスに書き込まれる
- この際ファイルシステムはブロック単位でデバイスに読み書きする (つまりブロックサイズ以下の読み書きは、ファイルシステムによって勝手にブロックサイズ単位に切り上げられてしまう)
- そのためアプリケーション (RDBMS) が書き込み単位をブロックサイズに切り上げてもほとんど同じになる
  - Linux の一般的なファイルシステムである `ext4` のデフォルトのブロックサイズは 4096 バイト (4KB)
  - 従ってページサイズも 4096 の整数倍にするのが一般的 (例えば MySQL ではデフォルトのページサイズは 16KB)

## ディスクの操作

各テーブルごとに 1 つのディスク (`Disk` struct) が存在し、そのテーブルのページの読み書き操作を行う

具体的に行う操作は以下の通り

### ページ ID を採番する

- 格納されるデータの単位 = 4096 バイトになるため、ファイルサイズを 4096 で割った値が次のページ番号になる
  - 例:
    - ファイルサイズが 0 バイトの場合、次のページ番号は 0
    - ファイルサイズが 4096 バイトの場合、次のページ番号は 1
    - ファイルサイズが 8192 バイトの場合、次のページ番号は 2

### ページの読み込み

- 指定された PageId に対応するページからデータを読み込む
- 読み込みの前に、ファイルディスクリプタをページの先頭へシークし、指定されたページサイズ分のデータを読み込む
  - 例: PageNumber が 2 の場合、ファイルディスクリプタを、ファイルの先頭位置から 8192 バイト (2 * 4096) へ移動してからデータを読み込む
  - 読み込むデータのサイズはページサイズ (4096 バイト) となる

### ページの書き込み

- 指定された PageId に対応するページにデータを書き込む
- 書き込みの前に、ファイルディスクリプタをページの先頭へシークし、指定されたデータを書き込む
  - 例: PageNumber が 2 の場合、ファイルディスクリプタを、ファイルの先頭位置から 8192 バイト (2 * 4096) へ移動してからデータを書き込む
  - 書き込むデータのサイズはページサイズ (4096 バイト) となる

### ページの Sync

- 前述の通り、minesql では OS のキャッシュを使用せずに独自のバッファプールを使用しているため、ディスクへの書き込みには O_DIRECT (`directio`) を使用している
- そのため、基本的にディスクが行うファイルの書き込みは、OS のキャッシュを経由せず直接ディスク (HDD/SSD) に書き込まれる
- ただし、ストレージ自体がデータをライトバックキャッシュに保持している可能性があり、それを考慮すると確実に書き込みを行うためには `fsync()` を呼び出す必要がある
  - 参考: https://lwn.net/Articles/457667/
  > I/O operations performed against files opened with O_DIRECT bypass the kernel's page cache, writing directly to the storage. Recall that the storage may itself store the data in a write-back cache, so fsync() is still required for files opened with O_DIRECT in order to save the data to stable storage. The O_DIRECT flag is only relevant for the system I/O API.
  - そのため、サーバーのプロセス停止時には `Sync()` を呼び出す方針としている
