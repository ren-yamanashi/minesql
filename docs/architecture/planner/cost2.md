# コストモデル (修正版)

MySQL のコストモデルを参考に、minesql のコストモデルを設計する。

## 参考文献

- [MySQL SQLオプティマイザのコスト計算アルゴリズム](https://dbstudy.info/files/20120310/mysql_costcalc.pdf)

## コストに使用する変数

| 変数名 | 説明 | デフォルト値 |
| --- | --- | --- |
| RowEvaluateCost | 1 レコードを評価するコスト | 0.1 |

これは MySQL の server_cost テーブルに格納されているコストパラメータを参考にしている

```sql
mysql> SELECT cost_name, cost_value, default_value FROM mysql.server_cost WHERE cost_name = 'row_evaluate_cost';
+------------------------------+------------+---------------+
| cost_name                    | cost_value | default_value |
+------------------------------+------------+---------------+
| row_evaluate_cost            |       NULL |           0.1 |
+------------------------------+------------+---------------+
```

## コストの算出

- SQL コストは以下の二つのパラメータから計算される
  - foundRecords: 読み取られるレコード数の推定値
  - readTime: ディスクアクセスの I/O コスト

### page_read_cost について

- コスト算出の際に `page_read_cost` という値を頻繁に使用する
- `page_read_cost` は、1 ページの読み取りコストをバッファプールのキャッシュヒット率に応じて重み付けした値 (参考: [opt_costmodel.cc#L79-L93](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/sql/opt_costmodel.cc#L79-L93))
  - page_read_cost = in_mem × 0.25 + (1 - in_mem) × 1.0
    - `in_mem`: バッファプールに載っている割合 (0.0 〜 1.0)
    - `0.25`: バッファプール内 (メモリ上) のページ読み取りコスト
    - `1.0`: ディスク上のページ読み取りコスト
    - 例えばテーブルの 100% がバッファプールにある場合は page_read_cost = 0.25、全くない場合は 1.0 となる
  - `in_mem` の計算方法 (InnoDB の場合, 参考: [ha_innodb.cc#L17159-L17172](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/storage/innobase/handler/ha_innodb.cc#L17159-L17172))
    - in_mem = n_in_mem / n_leaf
      - `n_in_mem`: バッファプール内の該当インデックスのページ数 (InnoDB 内部の `buf_stat_per_index` カウンタから取得)
      - `n_leaf`: 該当インデックスのリーフページ総数 (`stat_n_leaf_pages`)
  - `in_mem` の参照先はスキャンの種類によって異なる
    - index-only scan: セカンダリインデックスの in_mem を使う (参考: [opt_costmodel.cc#L95-L105](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/sql/opt_costmodel.cc#L95-L105))
    - 非 index-only scan / フルスキャン: クラスタ化インデックス の in_mem を使う (参考: [opt_costmodel.cc#L79-L93](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/sql/opt_costmodel.cc#L79-L93), [ha_innodb.cc#L17379-L17381](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/storage/innobase/handler/ha_innodb.cc#L17379-L17381))
  - ※ MySQL の Optimizer Trace の `in_memory` フィールドは常にインデックスの常駐率 (`KEY::in_memory_estimate()`) を表示する (参考: [index_range_scan_plan.cc#L908](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/sql/range_optimizer/index_range_scan_plan.cc#L908))。非 index-only scan でもインデックスの値が表示されるため、コスト計算に使われる主キーの常駐率とは異なる場合がある

## 単一テーブルのフルスキャン

```sql
-- first_name にインデックスがないと仮定
SELECT * FROM users WHERE first_name = 'John';
```

- foundRecords: テーブル統計情報の中の、「レコード数」を用いる
- readTime: scan_time × page_read_cost (参考: [handler.cc#L6030-L6042](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/sql/handler.cc#L6030-L6042))
  - `scan_time`: クラスタ化インデックスのページ数 (= データサイズ / ページサイズ)
  - `page_read_cost`: 1 ページの読み取りコスト (詳細: [page_read_cost について](#page_read_cost-について))
- コスト: readTime + foundRecords × RowEvaluateCost (参考: [sql_planner.cc#L1097-L1101](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/sql/sql_planner.cc#L1097-L1101))
  - ディスクアクセスの I/O コスト (`readTime`) とレコード評価の CPU コスト (`foundRecords × RowEvaluateCost`) を合算する

### 例

- 仮定
  - レコード数: 74822
  - データサイズ: 7880704
  - ページサイズ: 4096
  - page_read_cost: 1.0 (全データがディスク上)
- 計算式:
  - scan_time: 7880704 / 4096 = 1,924
  - readTime: 1,924 × 1.0 = 1,924
  - foundRecords: 74822
  - コスト: 1,924 + 74822 × 0.1 = 9,406.2

## 単一テーブルのユニークスキャン

```sql
--- username にユニークインデックスがあると仮定
SELECT * FROM users WHERE username = 'john-doe';
```

- コストは 1.0 固定とする
  - 参考: [sql_optimizer.cc#L5905-L5910](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/sql/sql_optimizer.cc#L5905-L5910)
- オプティマイザは、より良いキータイプ (e.g. UNIQUE キー) が既にあるなら、劣るキータイプのコスト計算を省略する
  - 参考: [sql_planner.cc#L412-L423](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/sql/sql_planner.cc#L412-L423)
  - 従って、UNIQUE インデックスが見つかった後、NOT_UNIQUE のインデックスはスキップされる

## 単一テーブルのレンジスキャン

(インデックスの貼られたカラムに対して範囲検索をした場合)

- PostgreSQL などの場合は、範囲検索に対してはヒストグラム統計を用いて読み取りレコード数を推定しているが、minesql では MySQL と同様にヒストグラム統計を用いない
- 範囲検索におけるレコード数の推定は、ストレージエンジンが担う (MySQL ではこの処理を「レンジ分析」と呼ぶので minesql でも同様に「レンジ分析」と呼ぶことにする)
  - 補足: MySQL ではストレージエンジン API の `records_in_range()` を呼び出すことでストレージエンジンに読み取りレコード数の見積もりを依頼する

### foundRecords の推定方法

1. プランナーがストレージエンジンに対して、レンジ分析の API を呼び出す
2. ストレージエンジンは、検索範囲の下限値と上限値を受け取り、それらが格納されているリーフページを読み取る
3. それぞれのリーフページの位置に応じて、読み取られるレコード数の推定値を算出する

#### 下限値と上限値が同じリーフページにある場合・・・

- リーフページを読み取ることで以下の情報を得ることができる
  - 該当のページに含まれるインデックスのエントリ数
  - それぞれ (下限値と上限値) のインデックスエントリが、ページのどの位置にあるのか
- 下限値、上限値におけるインデックスエントリの番号を `nth_rec_1`, `nth_rec_2` とすると、読み取られるレコード数の推定値は以下の式で表せる
  - foundRecords = nth_rec_2 - nth_rec_1 - 1 (参考: [btr0cur.cc#L5287](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/storage/innobase/btr/btr0cur.cc#L5287))
    - 左右どちらの境界もカウントしないため、-1 としている
    >　We do not count the borders (nor the left nor the right one), thus "- 1".
    - その後、検索条件の演算子に応じて左境界・右境界をそれぞれ +1 する (例: `<=` なら右境界を含むので +1、`<` なら +0) (参考: [btr0cur.cc#L5216-L5230](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/storage/innobase/btr/btr0cur.cc#L5216-L5230))

#### 上限値のインデックスエントリが、下限値のインデックスエントリと異なるリーフページにあり、かつそれらが隣接している場合・・・

- 下限値、上限値のリーフページに含まれるレコード数をそれぞれ `n_recs_1`, `n_recs_2` とし、また下限値、上限値におけるインデックスのエントリ番号をそれぞれ `nth_rec_1`, `nth_rec_2` とすると、読み取られるレコード数の推定値は以下の式で表せる (参考: [btr0cur.cc#L5312-L5326](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/storage/innobase/btr/btr0cur.cc#L5312-L5326))
  - (n_recs_1 - nth_rec_1) + (nth_rec_2 - 1)

#### 上限値のインデックスエントリが、下限値のインデックスエントリと異なるリーフページにあり、かつそれらが隣接していない場合・・・

- 下限値のページから上限値のページに向かってリンクリストを辿り、合計 10 ページまで読み取る (参考: [btr0cur.cc#L4914](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/storage/innobase/btr/btr0cur.cc#L4914))

- 10 ページ以内に上限値のインデックスエントリが見つかった場合・・・
  - 下限値、上限値のリーフページに含まれるレコード数をそれぞれ `n_recs_1`, `n_recs_2` とし、また下限値、上限値におけるインデックスのエントリ番号をそれぞれ `nth_rec_1`, `nth_rec_2` とすると、読み取られるレコード数の推定値は以下の式で表せる (参考: [btr0cur.cc#L4892-L4904](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/storage/innobase/btr/btr0cur.cc#L4892-L4904), [btr0cur.cc#L4964-L4968](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/storage/innobase/btr/btr0cur.cc#L4964-L4968))
    - (n_recs_1 - nth_rec_1) + Σ n_recs_mid + (nth_rec_2 - 1)

- 10 ページ以内に上限値のインデックスエントリが見つからなかった場合・・・
  - 読んだページから 1 ページあたりの平均レコード数を求め、それに対象範囲のページ数を掛けて全体のレコード数を推定する。この場合は「行数が確定していない扱い (mysql でいうところの `is_n_rows_exact = false`)」になる (参考: [btr0cur.cc#L4993-L5003](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/storage/innobase/btr/btr0cur.cc#L4993-L5003))
    - foundRecords = n_rows_on_prev_level × (n_rows / n_pages_read)
      - `n_rows / n_pages_read`
        - 読み取ったページから算出した 1 ページあたりの平均レコード数
      - `n_rows_on_prev_level`
        - B-tree の 1 つ上のレベル (ブランチノード) で推定された、対象範囲内のレコード数
        - ブランチノードの各レコードは境界キーと子ページへのポインタを持っており、1 レコードが下のレベルの 1 ページに対応する
        - そのため、上のレベルで下限値と上限値の間にあるレコード数を数えれば、それがそのまま現在のレベルにおける対象範囲の中間ページ数となる

#### foundRecords の後処理

上記で算出した推定値に対して、以下の補正が順に適用される。いずれも行数が確定していない場合 (`is_n_rows_exact = false`) にのみ適用される。

- x2 補正: B+Tree の高さが 1 より大きい場合、推定値を 2 倍にする (参考: [btr0cur.cc#L5234-L5239](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/storage/innobase/btr/btr0cur.cc#L5234-L5239))
  > In trees whose height is > 1 our algorithm tends to underestimate: multiply the estimate by 2
- 半数キャップ: 推定値がテーブルの総行数の半分を超えた場合、総行数の半分に切り詰める (参考: [btr0cur.cc#L5248-L5257](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/storage/innobase/btr/btr0cur.cc#L5248-L5257))

### readTime の推定方法

以下の式で共通して使用する変数:

- `n_ranges`: レンジ条件の区間の数
  - 例:
    - `WHERE x BETWEEN 10 AND 20` は `10 <= x <= 20` という 1 つの連続した区間なので 1
    - `WHERE x IN (1, 5, 10)` は `x = 1`, `x = 5`, `x = 10` という 3 つの独立した区間に分かれるので 3
- `foundRecords`: 前述の方法で推定されたレコード数
- `page_read_cost`: 1 ページの読み取りコスト (詳細: [page_read_cost について](#page_read_cost-について))

#### セカンダリインデックスの非 index-only scan の場合

- readTime = (n_ranges + foundRecords) × page_read_cost (参考: [handler.cc#L6075-L6077](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/sql/handler.cc#L6075-L6077))

#### index-only scan の場合

- readTime = index_only_read_time × page_read_cost (参考: [handler.cc#L6057-L6058](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/sql/handler.cc#L6057-L6058))
  - `index_only_read_time`: 読み取るページ数の推定値 (参考: [handler.cc#L5916-L5923](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/sql/handler.cc#L5916-L5923))
    - index_only_read_time = (foundRecords + keys_per_block - 1) / keys_per_block
      - keys_per_block = (block_size / 2 / (key_length + ref_length)) + 1
        - `block_size`: ページサイズ (InnoDB デフォルト: 16384)
        - `key_length`: インデックスキーのバイト長 (EXPLAIN の `key_len` に対応)
        - `ref_length`: 主キーのバイト長 (セカンダリインデックスのリーフには主キーの値が含まれるため)
  - `page_read_cost`: 1 ページの読み取りコスト (詳細: [page_read_cost について](#page_read_cost-について))

#### クラスタ化インデックスの場合

- クラスタ化インデックスではデータ行がインデックスのリーフに直接格納されているため、フルスキャンのコストに対する読み取り行数の比率で I/O コストを推定する (参考: [ha_innodb.cc#L16899](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/storage/innobase/handler/ha_innodb.cc#L16899))
  - readTime = (n_ranges + (foundRecords / total_rows) × scan_time) × page_read_cost
    - `total_rows`: テーブルの総行数の上限推定値
    - `scan_time`: クラスタ化インデックスのページ数 (参考: [ha_innodb.cc#L16840-L16868](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/storage/innobase/handler/ha_innodb.cc#L16840-L16868))
    - `foundRecords / total_rows`: 読み取る行がテーブル全体の何割かを表す比率
    - `page_read_cost`: 1 ページの読み取りコスト (詳細: [page_read_cost について](#page_read_cost-について))
  - ただし `foundRecords` が 2 以下の場合は readTime = foundRecords × page_read_cost となる (参考: [ha_innodb.cc#L16886-L16888](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/storage/innobase/handler/ha_innodb.cc#L16886-L16888))

### 最終的なコスト算出

MySQL ではレンジスキャンのコストが 2 段階で計算される。

1. レンジオプティマイザが readTime に CPU コストを加算する (参考: [handler.cc#L6297-L6298](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/sql/handler.cc#L6297-L6298))
   - rangeCost = readTime + foundRecords × RowEvaluateCost + 0.01
2. ジョインオプティマイザがさらにレコード評価コストを加算する (参考: [sql_planner.cc#L1153-L1155](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/sql/sql_planner.cc#L1153-L1155))
   - コスト = rangeCost + foundRecords × RowEvaluateCost

1 と 2 の各段階で `foundRecords × RowEvaluateCost` が 1 回ずつ、合計 2 回加算される。展開すると:

- コスト = readTime + foundRecords × RowEvaluateCost + 0.01 + foundRecords × RowEvaluateCost
- = readTime + 2 × foundRecords × RowEvaluateCost + 0.01

※ フルスキャンでは readTime に RowEvaluateCost が含まれないため 1 回のみの加算になる ([sql_planner.cc#L1097-L1101](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/sql/sql_planner.cc#L1097-L1101))。この差異はレンジオプティマイザとジョインオプティマイザが独立したコンポーネントであることに起因する。レンジオプティマイザの `multi_range_read_info_const` は全行分の CPU コストを含めて返すが ([handler.cc#L6297-L6298](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/sql/handler.cc#L6297-L6298))、フルスキャンの `table_scan_cost` は I/O コストのみを返す ([handler.cc#L6030-L6042](https://github.com/mysql/mysql-server/blob/89e1c722476deebc3ddc8675e779869f6da654c0/sql/handler.cc#L6030-L6042))。ジョインオプティマイザはどちらの場合もレコード評価コストを加算するため、レンジスキャンでは二重加算になる。

### 例

- 仮定
  - foundRecords: 500 (レンジ分析により推定)
  - n_ranges: 1 (`WHERE age BETWEEN 20 AND 30` のような単一レンジ)
  - セカンダリインデックスを使用
  - page_read_cost: 1.0 (全データがディスク上)
- 計算式:
  - readTime: (1 + 500) × 1.0 = 501
  - rangeCost: 501 + 500 × 0.1 + 0.01 = 551.01
  - コスト: 551.01 + 500 × 0.1 = 601.01
