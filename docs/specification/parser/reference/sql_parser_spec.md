# SQL パーサーの詳細仕様

- [sql_parser.md](../sql_parser.md) の論理モデルに対する詳細仕様
- MySQL 8.4 (commit `aa461240`) の `sql/sql_yacc.yy` (文法)、`sql/sql_lex.cc` と `strings/sql_chars.cc` (字句解析器)、`sql/lex.h` (キーワード表) を参照し、minesql の文法ファイルに写す規則と写さない規則を理由つきで記録する
- 構成: 構文の範囲の決め方 → 字句規則 → 演算子の優先順位 → 式の規則 → ステートメントごとの規則 → 構文エラー

## 構文の範囲の決め方

- 対象のステートメントは SELECT / INSERT / UPDATE / DELETE / CREATE TABLE / DROP TABLE / CREATE SCHEMA / DROP SCHEMA / USE / トランザクション制御 (BEGIN、START TRANSACTION、COMMIT、ROLLBACK) の 10 種
  - MySQL の `simple_statement` (ステートメントの一覧) の選択肢のうち、この 10 種に対応するものだけを写す
  - 参照:
    - [simple_statement](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2403)
    - [select_stmt の選択肢](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2480)、[insert_stmt](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2460)、[update_stmt](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2537)、[delete_stmt](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2432)
    - [create_table_stmt](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2430)、[drop_table_stmt](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2447)、[create (CREATE DATABASE を含む)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2425)、[drop_database_stmt](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2435)、[use](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2538)
    - [commit](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2424)、[rollback](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2478)、[start](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2531)、[begin_stmt (simple_statement_or_begin)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2397-L2400)
- 写す単位は `sql_yacc.yy` の規則 (非終端記号) で、規則ごとに選択肢を「採用する / 外す」に分ける
  - 採用した選択肢は、規則の名前と並びを変えずに写す (規則末尾のアクションだけを minesql の AST ノードの生成に置き換える)
  - 外した選択肢は書かないので、その構文は構文エラーになる (MySQL は受理するので、その分だけ応答が異なる)
- 外す理由は次の 7 つのどれかで、以降の表では記号で示す
  - A: [issue #120](https://github.com/ren-yamanashi/minesql/issues/120) の「やらないこと」に挙がっている機能の構文
  - B: 対応する実装が minesql にない機能の構文 (含めるかどうかは、実装コストと MySQL の仕組みを説明するうえでの重要度で個別に判断する)
  - C: 8.4 で非推奨 (deprecated) になっている別表記
  - D: MySQL 自身が読み飛ばす、または挙動に影響しない構文
  - E: `sql_mode` に依存する分岐のうち、8.4 の既定値では選ばれない側の挙動 (minesql は `sql_mode` を持たず、既定値の側に固定する)
  - F: 外した他の構文と組でしか意味を持たない構文
  - G: 標準 SQL にない MySQL 独自の別表記で、同じ意味の書き方が他にあるもの
- 未実装の機能でも、次の 4 つは実装コストが低く MySQL の仕組みの説明に効くため文法に含める (2026-09-11 確定、issue #120 から ORDER BY と LIMIT を外した)
  - SELECT の `ORDER BY`
    - インデックスの順序で満たすかソート (ファイルソート) するかはプリペア以降 (最適化) の判断で、文法は MySQL と同じ形を受理する
  - SELECT の `LIMIT`
  - `START TRANSACTION WITH CONSISTENT SNAPSHOT`
  - `SELECT ... FOR UPDATE` と `SELECT ... FOR SHARE` (`LOCK IN SHARE MODE` は `FOR SHARE` の同義語)
- 「B」で外したものは、後から実装すると決めた時点でこの表の行を「採用」に変え、示してある規則を写せばよい

## 字句規則

字句解析器は入力の文字列を先頭から読み、トークン (キーワード、識別子、リテラル、記号) を 1 つずつ返す。MySQL では文字の種類ごとに開始状態を引く表 (`state_map`) と、識別子に使える文字の表 (`ident_map`) を文字集合ごとに持ち、`lex_one_token` がその状態機械を回す

- 参照:
  - [sql_chars.cc の init_state_maps](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/strings/sql_chars.cc#L65-L131)
  - [sql_lex.cc の lex_one_token](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1436)
  - [my_sql_parser_lex (構文解析器から呼ばれる入口)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1367-L1434)

### 空白とコメント

- 空白文字 (文字集合の `isspace` に該当する文字) はトークンの区切りで、改行を数えて行番号を進める (構文エラーの位置に使う)
  - 参照:
    - [MY_LEX_START での空白の読み飛ばし](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1454-L1466)
- コメントは 3 種類で、いずれもトークンの区切りとして読み飛ばす
  - `/* ... */`: 入れ子にできない
    - 閉じる `*/` がなければ構文エラー
    - 参照:
      - [MY_LEX_LONG_COMMENT](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1886)
      - [閉じていないコメントの扱い](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1994)
      - [consume_comment](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1313-L1349)
  - `-- ` から行末まで: `--` の直後に空白か制御文字が必要で、`--x` はコメントにならない
    - 参照:
      - [MY_LEX_CHAR での `--` の判定](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1469-L1473)
  - `#` から行末まで
    - 参照:
      - [state_map の `#`](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/strings/sql_chars.cc#L100-L114)
      - [MY_LEX_COMMENT](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1879-L1885)
- MySQL 固有のコメントは、minesql では通常のコメントとして読み飛ばす
  - `/*!50708 ... */` (実行コメント): MySQL は 5 桁または 6 桁のバージョン番号が自身のバージョン以下なら中身を SQL として読む
    - minesql は MySQL のバージョン番号を持たないため、MySQL 以外のサーバーと同じく読み飛ばす
    - 参照:
      - [バージョンコメントの判定](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1897-L1960)
  - `/*+ ... */` (オプティマイザヒント): MySQL はヒント用の別の構文解析器に渡す
    - 外す理由 B
    - 参照:
      - [find_keyword でのヒントの読み取り](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L926-L933)

### 識別子

- 引用なしの識別子
  - 使える文字は英字、数字、`_`、`$`、および 0x80 以上のバイト (utf8mb4 のマルチバイト文字)
    - 参照:
      - [state_map の文字の分類](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/strings/sql_chars.cc#L87-L100)
      - [ident_map](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/strings/sql_chars.cc#L119-L122)
  - 数字で始まる語は、数値リテラルとして読み切れなければ識別子になる (`1abc` は識別子、`1e5` と `0x1F` はリテラル)
    - 参照:
      - [MY_LEX_NUMBER_IDENT](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1641-L1692)
  - `$` で始まる語は 8.4 で非推奨 (警告つきで受理) のため、minesql では採用しない (理由 C)
    - `$` は 2 文字目以降にだけ使える
    - 参照:
      - [MY_LEX_IDENT_OR_DOLLAR_QUOTED_TEXT の警告](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L2125-L2143)
  - 切り出した語がキーワード表にあればキーワード、なければ識別子 ([キーワード](#キーワード) を参照)
- 引用ありの識別子
  - バッククォートで囲む
    - 中でバッククォートを使うときは 2 つ重ねる
    - 中の語はキーワードでも識別子として扱う
    - 閉じるバッククォートがなければ構文エラー
    - 参照:
      - [MY_LEX_USER_VARIABLE_DELIMITER](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1730-L1769)
  - `"` による引用 (`sql_mode` の `ANSI_QUOTES`) は採用せず、`"` は常に文字列の引用符とする (理由 E)
    - 参照:
      - [MY_LEX_STRING_OR_DELIMITER](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1854-L1860)
- 修飾
  - 識別子の直後の `.` に識別子の文字が続くとき、次の語はキーワードであっても識別子として読む (`t.key` を引用なしで書ける)
    - 参照:
      - [MY_LEX_IDENT_SEP と MY_LEX_IDENT_START](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1625-L1639)
  - 採用する形は `テーブル名.列名`、`スキーマ名.テーブル名`、`スキーマ名.テーブル名.列名`、`テーブル名.*`、`スキーマ名.テーブル名.*` (MySQL の規則と同じ)
    - スキーマを持つ判断 (2026-09-12) より前は「スキーマがない」を理由にスキーマ名の修飾を外していた
    - 参照:
      - [table_ident](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14924-L14939)
      - [simple_ident_q](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14911-L14922)
      - [table_wild](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14867-L14879)
- 名前の妥当性 (空でないこと、末尾が空白でないこと、64 文字以内であること) の検査は字句規則に含めない
  - MySQL でも構文解析の後 (文脈化と DDL の処理) で検査している
  - minesql ではプリペア (名前解決) の責務とする
  - 参照:
    - [table.cc の check_table_name](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/table.cc#L3749-L3774)
    - [parse_tree_helpers.cc での ER_TOO_LONG_IDENT](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/parse_tree_helpers.cc#L430)
    - [mysql_com.h の NAME_CHAR_LEN](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/include/mysql_com.h#L60)

### キーワード

- 判定の仕方: 識別子と同じ規則で語を切り出したあと、キーワード表を大文字小文字を区別せずに引く
  - 表にあればその語のトークンを返し、なければ識別子のトークンを返す
  - MySQL は語の直後が `(` かどうかで関数名の表も引くが、minesql には関数がないのでこの区別を持たない
  - 参照:
    - [find_keyword](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L905-L936)
    - [識別子の切り出し後のキーワード判定](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1578-L1584)
    - [lex.h のキーワード表 symbols](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/lex.h#L61)
- 表の範囲 (2026-09-11 確定): minesql の文法に現れる語だけをキーワードにする
  - MySQL の予約語であっても minesql の文法に現れない語 (`WINDOW`、`CUBE` など) は、minesql では識別子として扱う
    - MySQL で通る SQL は minesql でも通る、という向きの互換は保たれる (minesql が予約する語は MySQL の予約語の部分集合であるため)
- 予約語と非予約語
  - MySQL では、キーワードのうち `ident_keyword` 規則に現れる語 (非予約語) は識別子としても使え、現れない語 (予約語) は引用しないと識別子に使えない
  - minesql でも同じ区分に従い、非予約語は `ident_keyword` 相当の規則で識別子として受理する
  - 参照:
    - [ident と ident_keyword](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L15092-L15102)
    - [ident_keyword とその分類の説明](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L15217-L15223)
    - [ident_keywords_unambiguous](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L15311)
- minesql のキーワード表 (区分は MySQL の `ident_keyword` に現れるかどうかで判定した)

| 使う場所 | 予約語 | 非予約語 (識別子にも使える) |
| --- | --- | --- |
| ステートメントの先頭と句 | `SELECT` `FROM` `WHERE` `INSERT` `INTO` `VALUES` `SET` `UPDATE` `DELETE` `CREATE` `DROP` `TABLE` `USE` | `BEGIN` `START` `TRANSACTION` `COMMIT` `ROLLBACK` `WORK` `TABLES` `VALUE` |
| テーブル参照 | `AS` `JOIN` `INNER` `CROSS` `ON` | |
| 式 | `AND` `OR` `XOR` `NOT` `IS` `TRUE` `FALSE` `IN` `BETWEEN` `LIKE` `DIV` `MOD` `CASE` `WHEN` `THEN` `ELSE` | `END` `ESCAPE` |
| テーブルとスキーマの定義 | `IF` `EXISTS` `PRIMARY` `KEY` `UNIQUE` `INDEX` `FOREIGN` `REFERENCES` `CONSTRAINT` `RESTRICT` `NULL` `VARCHAR` `VARCHARACTER` `CHAR` `VARYING` `DATABASE` `SCHEMA` | `NO` `ACTION` |
| ORDER BY / LIMIT / ロック読み取り / トランザクションのオプション | `ORDER` `BY` `ASC` `DESC` `LIMIT` `FOR` `LOCK` `WITH` | `OFFSET` `SHARE` `MODE` `CONSISTENT` `SNAPSHOT` |

- 補足
  - `NULL` は NULL リテラルとしては外すが (理由 A)、列属性 `NOT NULL` のためにキーワードとしては持つ
  - `VARCHARACTER` は MySQL のキーワード表で `VARCHAR` と同じトークンに写される同義語で、minesql でも同じ扱いにする
    - 参照:
      - [lex.h の VARCHAR / VARCHARACTER](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/lex.h#L778-L779)
  - `TABLES` は `DROP TABLES` の同義語として使う (`table_or_tables` 規則)
  - `SCHEMA` は MySQL のキーワード表で `DATABASE` と同じトークンに写される同義語で、minesql でも同じ扱いにする
    - 参照:
      - [lex.h の SCHEMA](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/lex.h#L607)

### リテラル

- 文字列
  - `'...'` と `"..."` の 2 つの引用符を使える (`"` は理由 E により常に文字列)
  - 同じ引用符を 2 つ重ねると 1 文字の引用符になる
  - バックスラッシュによるエスケープを解釈する (`sql_mode` の `NO_BACKSLASH_ESCAPES` は既定で無効のため、理由 E)
    - `\0` `\b` `\n` `\r` `\t` `\Z` はそれぞれ NUL、後退、改行、復帰、タブ、Ctrl-Z の 1 文字になる
    - `\%` と `\_` はバックスラッシュを残して 2 文字のまま保つ (LIKE のパターンで使うため)
    - それ以外の `\x` は `x` 1 文字になる (`\\` は `\`、`\'` は `'`)
  - 閉じる引用符がなければ構文エラー
  - 隣接する 2 つの文字列リテラルは 1 つに連結する (`'a' 'b'` は `'ab'`)
  - 参照:
    - [MY_LEX_STRING](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1861-L1877)
    - [get_text (エスケープの解釈)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1023-L1127)
    - [text_literal (隣接リテラルの連結)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14681-L14703)
- 数値
  - 数字の並び、小数 (`1.5`、`.5`)、指数表記 (`1e10`、`1.5E-3`) を読む
  - 符号は字句には含めず、式の単項演算子として扱う
  - トークンの種類は MySQL と同じ 5 つに分ける (規則がこの区別を前提にしているため)
    - `NUM`: 2147483647 以下の整数
    - `LONG_NUM`: 9223372036854775807 以下の整数
    - `ULONGLONG_NUM`: 18446744073709551615 以下の整数
    - `DECIMAL_NUM`: それより大きい整数と、小数
    - `FLOAT_NUM`: 指数表記
  - 参照:
    - [MY_LEX_NUMBER_IDENT](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1641-L1692)
    - [MY_LEX_INT_OR_REAL と MY_LEX_REAL](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1770-L1793)
    - [MY_LEX_REAL_OR_POINT (`.5` の形)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L2057-L2063)
    - [int_token (整数の分類)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1242-L1299)
- 真偽値: `TRUE` と `FALSE` (キーワード)
  - 参照:
    - [literal 規則の TRUE / FALSE](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14780-L14808)
- 採用しないリテラル

| リテラル | 理由 | 参照 |
| --- | --- | --- |
| `NULL` | A | [null_as_literal](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14765-L14778) |
| `N'...'` (national 文字列) | C | [text_literal の NCHAR_STRING](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14687-L14692) |
| `_utf8mb4'...'` (文字集合の指定) | B | [MY_LEX_IDENT でのアンダースコア文字集合](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1594) |
| `X'1F'` `0x1F` `B'101'` `0b101` (16 進・2 進のバイナリ文字列) | B | [MY_LEX_HEX_NUMBER / MY_LEX_BIN_NUMBER](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1795-L1818) |
| `DATE '...'` `TIME '...'` `TIMESTAMP '...'` | B | [temporal_literal](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14838-L14851) |
| `$tag$...$tag$` (ドル引用) | B (ストアドプログラムの本体でだけ使う) | [routine_string](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L17909-L17911) |
| `?` (プレースホルダ) | X Plugin は `args` を埋め込んでからステートメントを渡すため、パーサーに届かない | [MY_LEX_CHAR での `?` の条件 (プリペアドステートメントの準備時のみ)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1494) |

### 記号と演算子

- 1 文字の記号: `(` `)` `,` `.` `;` `*` `+` `-` `/` `%` `=` `<` `>` `&` `|` `^` `~`
- 2 文字以上の演算子: `<=` `>=` `<>` `!=` `<<` `>>`
  - MySQL は `<` `>` `=` `!` で始まる並びを最長一致で切り出し、キーワード表 (演算子も登録されている) で引く
  - 参照:
    - [MY_LEX_CMP_OP と MY_LEX_LONG_CMP_OP](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1820-L1842)
    - [lex.h の演算子の登録](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/lex.h#L65-L75)
- 採用しない記号

| 記号 | 理由 | 参照 |
| --- | --- | --- |
| `<=>` (NULL 安全な等号) | A | [comp_op の EQUAL_SYM](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10336-L10344) |
| `&&` (AND の別表記) | C | [and 規則の非推奨警告](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10318-L10324) |
| `\|\|` (既定では OR の別表記、`PIPES_AS_CONCAT` では連結) | C と E | [find_keyword での OR2_SYM への置き換え](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L920-L924) |
| `!` (NOT の別表記) | C | [not2 規則の非推奨警告](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10331-L10334) |
| `:=` (変数への代入) | B | [MY_LEX_SET_VAR](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L2027-L2033) |
| `@` (ユーザー変数・システム変数) | B | [MY_LEX_USER_END](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L2065-L2080) |
| `->` `->>` (JSON パス) | B | [MY_LEX_CHAR での `->`](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1476-L1483) |
| `{` `}` (ODBC エスケープ) | B | [simple_expr の `'{' ident expr '}'`](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10410-L10413) |

### ステートメントの終端

- 1 回の解析で受け取るのは 1 つのステートメントで、末尾の `;` は省略できる
  - `;` の後ろに別のステートメントが続いていれば構文エラー
  - MySQL では `;` の後ろに続きを許すかをクライアントの capability `CLIENT_MULTI_QUERIES` で決めており、X Plugin はこの capability を持たないため、X Protocol 経由では MySQL でも同じ挙動になる
  - 参照:
    - [sql_statement 規則](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2347-L2390)
    - [X Plugin が名乗るクライアントの capability](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/plugin/x/src/streaming_command_delegate.cc#L371-L374)
    - [Protocol_callback::has_client_capability](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/protocol_callback.cc#L190-L193)
- 入力の終わりは字句解析器が終端のトークン (`END_OF_INPUT`) として返す
  - 参照:
    - [MY_LEX_EOL](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L2037-L2049)
- 空の入力
  - 空白だけの入力は「空のクエリ」のエラー (`ER_EMPTY_QUERY`) になる
  - コメントだけの入力はエラーにせず、何もしないステートメントとして成功する
  - 参照:
    - [sql_statement の END_OF_INPUT の選択肢](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2348-L2358)

## 演算子の優先順位

- MySQL は式の演算子の優先順位と結合性を、文法ファイル冒頭の `%left` / `%right` / `%nonassoc` 宣言で与えている (下の行ほど優先順位が高い)
  - 宣言には実在のトークンのほかに、規則の `%prec` で優先順位を借りるためだけの擬似トークン (`CONDITIONLESS_JOIN`、`NEG`、`PREFER_PARENTHESES`、`EMPTY_FROM_CLAUSE`、`KEYWORD_USED_AS_IDENT` / `KEYWORD_USED_AS_KEYWORD`) が混じっている
  - minesql の文法ファイルにはこの宣言を同じ順序で転記し、外した構文にしか使わない行を除く
  - 参照:
    - [優先順位の宣言](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L1479-L1515)
    - [KEYWORD_USED_AS_IDENT の説明](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L1463-L1478)
    - [CONDITIONLESS_JOIN の説明 (joined_table の前のコメント)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11784-L11861)
    - [PREFER_PARENTHESES の説明 (query_expression の前のコメント)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9737-L9768)
    - [EMPTY_FROM_CLAUSE と INTO の説明 (select_stmt_with_into の前のコメント)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9685-L9716)

| 行 | MySQL の宣言 | minesql | 備考 |
| --- | --- | --- | --- |
| 1479 | `%left KEYWORD_USED_AS_IDENT` | 外す | `BIT` 型など、`%prec KEYWORD_USED_AS_KEYWORD` を使う規則を写さない (B) |
| 1480 | `%nonassoc TEXT_STRING` | 外す | 同上 |
| 1481 | `%left KEYWORD_USED_AS_KEYWORD` | 外す | 同上 |
| 1488 | `%right UNIQUE_SYM KEY_SYM` | 採用 | 列属性の `UNIQUE KEY` を `UNIQUE` と `KEY` の 2 属性より優先させる |
| 1490 | `%left UNION_SYM EXCEPT_SYM` | 外す | A (UNION)、B (EXCEPT) |
| 1491 | `%left INTERSECT_SYM` | 外す | B |
| 1492 | `%left CONDITIONLESS_JOIN` | 採用 | 条件のない JOIN の還元を、後続の `ON` を見るまで遅らせる擬似トークン |
| 1493 | `%left JOIN_SYM INNER_SYM CROSS STRAIGHT_JOIN NATURAL LEFT RIGHT ON_SYM USING` | 一部 | `JOIN` `INNER` `CROSS` `ON` を採用し、`STRAIGHT_JOIN` `NATURAL` `USING` (B)、`LEFT` `RIGHT` (A) を外す |
| 1494 | `%left SET_VAR` | 採用 (擬似トークンとして) | `:=` は持たないが、式の階層の単位規則 (`expr: bool_pri %prec SET_VAR` など) がこの段を使うため、段だけを残す |
| 1495 | `%left OR_SYM OR2_SYM` | 一部 | `OR` を採用し、`OR2_SYM` (`\|\|` を OR と読む挙動) を外す (C) |
| 1496 | `%left XOR` | 採用 | |
| 1497 | `%left AND_SYM AND_AND_SYM` | 一部 | `AND` を採用し、`&&` を外す (C) |
| 1498 | `%left BETWEEN_SYM CASE_SYM WHEN_SYM THEN_SYM ELSE` | 採用 | |
| 1499 | `%left EQ EQUAL_SYM GE GT_SYM LE LT NE IS LIKE REGEXP IN_SYM` | 一部 | `<=>` (`EQUAL_SYM`、A) と `REGEXP` (B) を外す |
| 1500 | `%left '\|'` | 採用 | |
| 1501 | `%left '&'` | 採用 | |
| 1502 | `%left SHIFT_LEFT SHIFT_RIGHT` | 採用 | |
| 1503 | `%left '-' '+'` | 採用 | |
| 1504 | `%left '*' '/' '%' DIV_SYM MOD_SYM` | 採用 | |
| 1505 | `%left '^'` | 採用 | |
| 1506 | `%left OR_OR_SYM` | 外す | `\|\|` を連結と読む挙動 (E) |
| 1507 | `%left NEG '~'` | 採用 | 単項マイナスとプラスが `%prec NEG` で借りる段 |
| 1508 | `%right NOT_SYM NOT2_SYM` | 一部 | `NOT` を採用し、`NOT2_SYM` (`!`、および `HIGH_NOT_PRECEDENCE` での `NOT`) を外す (C、E) |
| 1509 | `%right BINARY_SYM COLLATE_SYM` | 外す | B (文字集合と照合順序) |
| 1510 | `%left INTERVAL_SYM` | 外す | B (日時演算) |
| 1511 | `%left PREFER_PARENTHESES` | 外す | サブクエリ (A) と省略可能な型の長さ (B) の曖昧さ解消にだけ使う |
| 1512 | `%left '(' ')'` | 外す | 同上 |
| 1514 | `%left EMPTY_FROM_CLAUSE` | 外す | `INTO` 句との曖昧さ解消にだけ使う (F) |
| 1515 | `%right INTO` | 外す | `INTO` 句 (B) |

- MySQL は `%expect 59` で 59 個の shift/reduce 衝突を許容している
  - minesql は `%expect` を書かず、衝突 0 を生成時の合格条件にする
  - 外した宣言を除いたことで衝突が出た場合は、この表の判断を見直して更新する (宣言を黙って戻さない)
  - 参照:
    - [%expect](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L567)
- goyacc は `%left` / `%right` / `%nonassoc` / `%prec` / `%token` / `%type` / `%union` / `%start` を受け付け、Bison の `%empty` と `%expect` は受け付けない
  - 空の選択肢は `%empty` の代わりに `/* empty */` と書く
  - 衝突は生成時に標準出力へ `conflicts: N shift/reduce, M reduce/reduce` と出る
  - 参照:
    - [goyacc の指示子の表 (yacc.go)](https://github.com/golang/tools/blob/v0.31.0/cmd/goyacc/yacc.go#L313-L326)
    - [goyacc の衝突の報告 (yacc.go)](https://github.com/golang/tools/blob/v0.31.0/cmd/goyacc/yacc.go#L3018-L3030)

## 式の規則

MySQL の式は `expr` (論理演算) → `bool_pri` (比較) → `predicate` (IN / BETWEEN / LIKE など) → `bit_expr` (算術・ビット演算) → `simple_expr` (項) の 5 段の規則で書かれている。各段の選択肢ごとに採用の可否を示す

### expr

- 参照:
  - [expr](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10077-L10126)
  - [or](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10313-L10316)、[and](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10318-L10324)、[not](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10326-L10329)

| 選択肢 | minesql | 備考 |
| --- | --- | --- |
| `expr or expr` | 採用 | `or` は `OR` のみ (`OR2_SYM` は C) |
| `expr XOR expr` | 採用 | |
| `expr and expr` | 採用 | `and` は `AND` のみ (`&&` は C) |
| `NOT expr` | 採用 | `not` は `NOT` のみ (`NOT2_SYM` は C) |
| `bool_pri IS [not] TRUE` / `IS [not] FALSE` | 採用 | |
| `bool_pri IS [not] UNKNOWN` | 外す | A (NULL) |
| `bool_pri` | 採用 | `%prec SET_VAR` を含めて写す |

### bool_pri

- 参照:
  - [bool_pri](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10128-L10155)
  - [comp_op](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10336-L10344)、[all_or_any](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10346-L10349)

| 選択肢 | minesql | 備考 |
| --- | --- | --- |
| `bool_pri IS [not] NULL` | 外す | A |
| `bool_pri comp_op predicate` | 採用 | `comp_op` は `=` `>=` `>` `<=` `<` `<>` (`!=` は同じトークン) で、`<=>` を外す (A) |
| `bool_pri comp_op all_or_any table_subquery` | 外す | A (サブクエリ) |
| `predicate` | 採用 | |

### predicate

- 参照:
  - [predicate](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10157-L10246)
  - [expr_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11699-L11715)

| 選択肢 | minesql | 備考 |
| --- | --- | --- |
| `bit_expr [not] IN table_subquery` | 外す | A |
| `bit_expr [not] IN '(' expr ')'` | 採用 | MySQL は 1 要素の IN を専用のノード (`PTI_handle_sql2003_note184_exception`) で扱うが、minesql の AST では要素数 1 の IN として持つ |
| `bit_expr [not] IN '(' expr ',' expr_list ')'` | 採用 | |
| `bit_expr MEMBER [OF] '(' simple_expr ')'` | 外す | B (JSON) |
| `bit_expr [not] BETWEEN bit_expr AND predicate` | 採用 | |
| `bit_expr SOUNDS LIKE bit_expr` | 外す | B (関数) |
| `bit_expr [not] LIKE simple_expr [ESCAPE simple_expr]` | 採用 | |
| `bit_expr [not] REGEXP bit_expr` | 外す | B (正規表現) |
| `bit_expr` | 採用 | |

### bit_expr

- 参照:
  - [bit_expr](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10253-L10311)

| 選択肢 | minesql | 備考 |
| --- | --- | --- |
| `bit_expr '\|' bit_expr`、`'&'`、`SHIFT_LEFT`、`SHIFT_RIGHT`、`'^'` | 採用 | ビット演算 |
| `bit_expr '+' bit_expr`、`'-'`、`'*'`、`'/'`、`'%'`、`DIV`、`MOD` | 採用 | 算術演算 (`%` と `MOD` は同じ剰余) |
| `bit_expr '+' INTERVAL expr interval`、`'-' INTERVAL ...` | 外す | B (日時演算) |
| `simple_expr` | 採用 | |

### simple_expr

- 参照:
  - [simple_expr](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10351-L10487)
  - [simple_ident](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14895-L14901)、[simple_ident_q](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14911-L14922)
  - [literal_or_null](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14810-L14813)、[literal](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14780-L14808)、[NUM_literal](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14815-L14825)、[int64_literal](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14831-L14835)
  - [opt_expr](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11738-L11741)、[when_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11748-L11763)、[opt_else](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11743-L11746)

| 選択肢 | minesql | 備考 |
| --- | --- | --- |
| `simple_ident` (`ident`、`ident '.' ident`、`ident '.' ident '.' ident`) | 採用 | |
| `function_call_keyword` / `function_call_nonkeyword` / `function_call_conflict` / `function_call_generic` | 外す | B (関数) |
| `simple_expr COLLATE ident_or_text` | 外す | B |
| `literal_or_null` | 一部 | `literal` のうち文字列、数値、`TRUE` / `FALSE` を採用し、`NULL` (A)、16 進・2 進・文字集合つき・日時のリテラル (B) を外す |
| `param_marker` | 外す | プレースホルダは届かない ([リテラル](#リテラル) を参照) |
| `rvalue_system_or_user_variable` / `in_expression_user_variable_assignment` | 外す | B (変数) |
| `set_function_specification` (集約関数) / `window_func_call` | 外す | A |
| `simple_expr OR_OR_SYM simple_expr` | 外す | E |
| `'+' simple_expr`、`'-' simple_expr`、`'~' simple_expr` | 採用 | `%prec NEG` を含めて写す |
| `not2 simple_expr` | 外す | C (`!`) |
| `row_subquery` / `EXISTS table_subquery` | 外す | A |
| `'(' expr ')'` | 採用 | |
| `'(' expr ',' expr_list ')'` / `ROW '(' expr ',' expr_list ')'` (行の値) | 外す | B (行どうしの比較) |
| `'{' ident expr '}'` (ODBC エスケープ) | 外す | B |
| `MATCH ... AGAINST` | 外す | B (全文検索) |
| `BINARY simple_expr` | 外す | C |
| `CAST (...)` / `CONVERT (...)` | 外す | B (型変換と文字集合) |
| `CASE opt_expr when_list opt_else END` | 採用 | 単純 CASE (`CASE x WHEN ...`) と検索 CASE (`CASE WHEN ...`) の両方 |
| `DEFAULT '(' simple_ident ')'` / `VALUES '(' simple_ident_nospvar ')'` | 外す | B (列の既定値、`ON DUPLICATE KEY UPDATE`) |
| `INTERVAL expr interval '+' expr` | 外す | B |
| `simple_ident JSON_SEPARATOR_SYM ...` / `JSON_UNQUOTED_SEPARATOR_SYM ...` | 外す | B (JSON) |

## ステートメントごとの規則

各ステートメントについて、写す規則の連なりと、規則ごとの「採用する選択肢 / 外す選択肢」を示す。表の「外す」列の括弧は [構文の範囲の決め方](#構文の範囲の決め方) の理由の記号

### ステートメントの入口

- MySQL の開始規則 `start_entry` は、通常の SQL のほかに、パーティション式や生成列の式だけを解析する入口 (`GRAMMAR_SELECTOR_*`) を持つ
  - minesql は `sql_statement` だけを開始規則にする
- `sql_statement` は 3 つの選択肢 (入力が空、`;` で終わる、`;` なしで終わる) を持ち、そのまま写す
  - ただし `;` の後ろで次のステートメントの解析を続ける処理 (`CLIENT_MULTI_QUERIES` 用) は写さない ([ステートメントの終端](#ステートメントの終端) を参照)
- `simple_statement_or_begin` が `BEGIN` を他のステートメントと分けているのは、ストアドプログラムの複合文 `BEGIN ... END` と区別するため
  - minesql にはトランザクションの `BEGIN` しかないため、この分岐は持たず、`simple_statement` の選択肢に `begin_stmt` を並べる
- 参照:
  - [start_entry](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2301-L2345)
  - [sql_statement](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2347-L2390)
  - [simple_statement_or_begin](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2397-L2400)

### SELECT

- 規則の連なり: `select_stmt` → `query_expression` → `query_expression_body` → `query_primary` → `query_specification`
  - MySQL がこの段数を持つのは UNION / ORDER BY / LIMIT / 括弧つきのクエリ式を扱うためで、minesql では `query_expression` に ORDER BY と LIMIT が残るほかは、どの段も選択肢が 1 つになる
  - 規則の名前は MySQL と対応づけるために残す

| 規則 | 採用する選択肢 | 外す選択肢 |
| --- | --- | --- |
| [select_stmt](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9672-L9683) | `query_expression`、`query_expression locking_clause_list` | `select_stmt_with_into` (B: `INTO`) |
| [query_expression](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9769-L9783) | `query_expression_body opt_order_clause opt_limit_clause` | `with_clause` で始まる選択肢 (B: CTE) |
| [query_expression_body](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9785-L9818) | `query_primary` | `query_expression_parens` (F: 集合演算と組)、`UNION` (A)、`EXCEPT` / `INTERSECT` (B) |
| [query_primary](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9831-L9851) | `query_specification` | `table_value_constructor` (`VALUES ROW(...)`、B)、`explicit_table` (`TABLE t`、B) |
| [query_specification](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9853-L9903) | 2 番目の選択肢を `SELECT select_item_list opt_from_clause opt_where_clause` に縮めたもの | 1 番目の選択肢 (`into_clause` 入り、B)、`select_options` (下記)、`opt_group_clause` / `opt_having_clause` (A)、`opt_window_clause` (A)、`opt_qualify_clause` (B) |

- 選択リスト

| 規則 | 採用する選択肢 | 外す選択肢 |
| --- | --- | --- |
| [select_item_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10031-L10052) | 3 つすべて (`select_item_list ',' select_item`、`select_item`、`'*'`) | |
| [select_item](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10054-L10060) | `table_wild` (`t.*`)、`expr select_alias` | |
| [select_alias](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10063-L10069) | 5 つすべて (省略、`AS ident`、`AS '文字列'`、`ident`、`'文字列'`) | |

- FROM 句とテーブル参照

| 規則 | 採用する選択肢 | 外す選択肢 |
| --- | --- | --- |
| [opt_from_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9905-L9908) | 省略 (`%prec EMPTY_FROM_CLAUSE` は付けない)、`from_clause` | |
| [from_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9910-L9912)、[from_tables](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9914-L9917) | `FROM table_reference_list` | `FROM DUAL` (G: FROM を省略した形と同じ) |
| [table_reference_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9919-L9932) | 2 つすべて (`,` 区切りの並び) | |
| [table_reference](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11765-L11778) | `table_factor`、`joined_table` | `'{' OJ esc_table_reference '}'` (B: ODBC) |
| [joined_table](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11862-L11894) | `table_reference inner_join_type table_reference ON expr`、`table_reference inner_join_type table_reference %prec CONDITIONLESS_JOIN` | `USING '(' using_list ')'` の 2 つ (B: 結合列の暗黙の解決)、`outer_join_type` の 2 つ (A)、`natural_join_type` (B) |
| [inner_join_type](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11902-L11906) | `JOIN`、`INNER JOIN`、`CROSS JOIN` (いずれも同じ内部結合) | `STRAIGHT_JOIN` (B: 結合順序の指示) |
| [table_factor](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11961-L11970) | `single_table`、`single_table_parens`、`joined_table_parens` | `derived_table` (A)、`table_reference_list_parens` (G: `(t1, t2)` は `t1 CROSS JOIN t2` と同じ)、`table_function` (B: `JSON_TABLE`) |
| [single_table](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11987-L11992) | `table_ident opt_table_alias` | `opt_use_partition` (A)、`opt_key_definition` (B: インデックスヒント)、`opt_tablesample_clause` (B) |
| [single_table_parens](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11982-L11985)、[joined_table_parens](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11994-L11997) | 2 つずつすべて (括弧の入れ子) | |
| [opt_table_alias](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12305-L12308)、[opt_as](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12300-L12303) | 省略、`[AS] ident` | |
| [table_ident](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14924-L14939) | `ident`、`ident '.' ident` (スキーマ名で修飾) | |

- WHERE 句: [opt_where_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12315-L12318) と [where_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12320-L12322) をそのまま写す
- ORDER BY 句と LIMIT 句 (`query_expression` の末尾に置く)

| 規則 | 採用する選択肢 | 外す選択肢 |
| --- | --- | --- |
| [opt_order_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12527-L12530)、[order_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12532-L12537) | 省略、`ORDER BY order_list` | |
| [order_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12539-L12553)、[order_expr](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14881-L14886) | `,` 区切りの `expr opt_ordering_direction` | |
| [opt_ordering_direction](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12555-L12558)、[ordering_direction](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12560-L12563) | 省略、`ASC`、`DESC` | |
| [opt_limit_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12565-L12568)、[limit_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12570-L12575) | 省略、`LIMIT limit_options` | |
| [limit_options](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12577-L12596) | 3 つすべて (`limit_option`、`limit_option ',' limit_option`、`limit_option OFFSET limit_option`) | |
| [limit_option](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12598-L12619) | `NUM`、`LONG_NUM`、`ULONGLONG_NUM` | `ident` (B: ストアドプログラムの変数)、`param_marker` (プレースホルダは届かない) |

- ロック読み取り (`FOR UPDATE` / `FOR SHARE`)

| 規則 | 採用する選択肢 | 外す選択肢 |
| --- | --- | --- |
| [locking_clause_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9982-L9995) | 2 つすべて (`locking_clause` の並び) | |
| [locking_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9997-L10010) | `FOR lock_strength opt_locked_row_action`、`LOCK IN SHARE MODE` (`FOR SHARE` の同義語) | `FOR lock_strength table_locking_list opt_locked_row_action` (B: `OF テーブル並び`) |
| [lock_strength](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10012-L10015) | `UPDATE` (排他ロック)、`SHARE` (共有ロック) | |
| [opt_locked_row_action](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10021-L10024) | 省略 | `locked_row_action` (`NOWAIT` / `SKIP LOCKED`、B) |

- 外した句と選択肢の一覧

| 句 / 選択肢 | 規則 | 理由 |
| --- | --- | --- |
| `DISTINCT` | [select_options](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9952-L9958)、[query_spec_option](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L17362) | B (重複の除去) |
| `ALL` | 同上 | F (`DISTINCT` と対) |
| `HIGH_PRIORITY` `STRAIGHT_JOIN` `SQL_SMALL_RESULT` `SQL_BIG_RESULT` `SQL_BUFFER_RESULT` | 同上 | B (実行の指示) |
| `SQL_CALC_FOUND_ROWS` | 同上 | C |
| `SQL_NO_CACHE` | [select_option](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9969-L9980) | D (8.0 以降は無視される) |
| `INTO` 句 (`INTO OUTFILE` / `INTO DUMPFILE` / `INTO 変数`) | [into_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12695) | B |
| `FOR UPDATE OF テーブル並び` | [table_locking_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10017-L10019) | B |
| `NOWAIT` / `SKIP LOCKED` | [locked_row_action](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L10026-L10029) | B |
| `GROUP BY` / `HAVING` | [opt_group_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12455)、[opt_having_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12324) | A |
| `WINDOW` | [opt_window_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12413) | A |
| `QUALIFY` | [opt_qualify_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12332) | B |
| `WITH` (CTE) | [with_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12340) | B |
| `UNION` / `EXCEPT` / `INTERSECT` と括弧つきクエリ式 | [query_expression_body](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9785-L9818)、[query_expression_parens](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9820-L9829) | A、B、F |
| `VALUES ROW(...)` / `TABLE t` | [table_value_constructor](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9934)、[explicit_table](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9941) | B |
| `FROM DUAL` | [from_tables](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9914-L9917) | G |
| 導出テーブル (`FROM (SELECT ...) AS t`) | [derived_table](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11999) | A |
| `JSON_TABLE(...)` | [table_function](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12023) | B |
| `(t1, t2)` (括弧つきのテーブル並び) | [table_reference_list_parens](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11972-L11980) | G |
| `{ OJ ... }` | [table_reference](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11765-L11778) | B |
| `LEFT [OUTER] JOIN` / `RIGHT [OUTER] JOIN` | [outer_join_type](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11908-L11911) | A |
| `NATURAL [INNER \| LEFT \| RIGHT] JOIN` | [natural_join_type](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11896-L11900) | B |
| `JOIN ... USING (列並び)` | [joined_table](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11862-L11894)、[using_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12241) | B |
| `STRAIGHT_JOIN` | [inner_join_type](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11902-L11906) | B |
| `PARTITION (...)` | [opt_use_partition](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11927-L11930) | A |
| インデックスヒント (`USE INDEX` など) | [opt_key_definition](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12197-L12199) | B |
| `TABLESAMPLE` | [opt_tablesample_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11097) | B |

### INSERT

| 規則 | 採用する選択肢 | 外す選択肢 |
| --- | --- | --- |
| [insert_stmt](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13064-L13120) | 1 番目 (`INSERT opt_INTO table_ident insert_from_constructor`) と 2 番目 (`INSERT opt_INTO table_ident SET update_list`) | 3 番目 (`insert_query_expression`、B: `INSERT ... SELECT`)、`insert_lock_option`、`opt_ignore`、`opt_use_partition`、`opt_values_reference`、`opt_insert_update_list` (下記) |
| [opt_INTO](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13196-L13199) | 省略、`INTO` | |
| [insert_from_constructor](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13201-L13218) | 3 つすべて (列並びなし、`()`、`(insert_columns)`) | |
| [insert_columns](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13238-L13252)、[insert_column](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14863-L14865) | `,` 区切りの `simple_ident_nospvar` (`ident`、`ident '.' ident`) | |
| [insert_values](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13254-L13259)、[value_or_values](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13269-L13272) | `VALUES values_list`、`VALUE values_list` (同義語) | |
| [values_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13274)、[row_value](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13315-L13317)、[opt_values](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13323-L13331)、[values](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13333-L13347) | `,` 区切りの `'(' opt_values ')'` (空の `()` を含む) | |
| [expr_or_default](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13349-L13355) | `expr` | `DEFAULT` (B: 列の既定値) |
| [update_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13411-L13428)、[update_elem](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13430-L13436)、[equal](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13305-L13308) | `,` 区切りの `simple_ident_nospvar '=' expr` | `:=` (B) |

- `INSERT ... SET col = val, ...` は 1 行の `INSERT ... (col, ...) VALUES (val, ...)` と同じ意味の別表記で、MySQL も構文解析時に 1 行の値並びに組み替えている
  - minesql のパーサーも同じく列並びと 1 行の値並びに組み替え、AST では両者を区別しない (構文上の同義語の吸収)
- 外した句と選択肢の一覧

| 句 / 選択肢 | 規則 | 理由 |
| --- | --- | --- |
| `LOW_PRIORITY` / `HIGH_PRIORITY` | [insert_lock_option](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13168-L13181) | B (ロックの優先度) |
| `DELAYED` | 同上 | D (MySQL は警告を出して通常の INSERT に読み替える) |
| `IGNORE` | [opt_ignore](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9036-L9039) | B (エラーの警告への格下げ) |
| `PARTITION (...)` | [opt_use_partition](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11927-L11930) | A |
| `INSERT ... SELECT` | [insert_query_expression](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13220-L13236) | B |
| `AS 別名 [(列並び)]` (行の別名) | [opt_values_reference](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13357-L13373) | F (`ON DUPLICATE KEY UPDATE` と組) |
| `ON DUPLICATE KEY UPDATE` | [opt_insert_update_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13375) | B |
| 値の `DEFAULT` | [expr_or_default](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13349-L13355) | B |

### UPDATE

| 規則 | 採用する選択肢 | 外す選択肢 |
| --- | --- | --- |
| [update_stmt](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13389-L13404) | `UPDATE single_table SET update_list opt_where_clause` | `opt_with_clause` (B)、`opt_low_priority` (B)、`opt_ignore` (B)、`table_reference_list` を `single_table` に置き換え (B: 複数テーブルの UPDATE)、`opt_order_clause` / `opt_simple_limit` (B: ORDER BY / LIMIT を含めるのは SELECT だけ) |
| [update_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13411-L13428)、[update_elem](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13430-L13436) | `,` 区切りの `simple_ident_nospvar '=' expr` | `:=` (B)、値の `DEFAULT` (B) |

- MySQL は単一テーブルと複数テーブルの UPDATE を同じ規則 (`table_reference_list`) で受理し、後の段階で区別する
  - minesql は複数テーブルの UPDATE を実装しないため、テーブル参照の位置に `single_table` (`table_ident opt_table_alias`) を置く
- 外した句と選択肢の一覧

| 句 / 選択肢 | 規則 | 理由 |
| --- | --- | --- |
| `WITH` (CTE) | [opt_with_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13406-L13409) | B |
| `LOW_PRIORITY` | [opt_low_priority](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13438-L13441) | B |
| `IGNORE` | [opt_ignore](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9036-L9039) | B |
| 複数テーブル (`UPDATE t1 JOIN t2 ...`、`UPDATE t1, t2 ...`) | [update_stmt の table_reference_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13389-L13404) | B |
| `ORDER BY` / `LIMIT` | [opt_order_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12527)、[opt_simple_limit](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12621-L12624) | B (2026-09-11 に含めたのは SELECT の ORDER BY / LIMIT だけで、更新対象の行を並べて絞る実装はない) |

### DELETE

| 規則 | 採用する選択肢 | 外す選択肢 |
| --- | --- | --- |
| [delete_stmt](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13445-L13480) | 1 番目を `DELETE FROM table_ident opt_table_alias opt_where_clause` に縮めたもの | `opt_with_clause` (B)、`opt_delete_options` (B)、`opt_use_partition` (A)、`opt_order_clause` / `opt_simple_limit` (B: ORDER BY / LIMIT を含めるのは SELECT だけ)、2 番目と 3 番目 (複数テーブルの DELETE、B) |

- 外した句と選択肢の一覧

| 句 / 選択肢 | 規則 | 理由 |
| --- | --- | --- |
| `WITH` (CTE) | [opt_with_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13406-L13409) | B |
| `QUICK` / `LOW_PRIORITY` / `IGNORE` | [opt_delete_options](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13487-L13496) | B |
| `PARTITION (...)` | [opt_use_partition](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L11927-L11930) | A |
| `ORDER BY` / `LIMIT` | [opt_order_clause](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12527)、[opt_simple_limit](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12621-L12624) | B (2026-09-11 に含めたのは SELECT の ORDER BY / LIMIT だけで、更新対象の行を並べて絞る実装はない) |
| 複数テーブル (`DELETE t1 FROM t1 JOIN t2 ...`、`DELETE FROM t1 USING ...`) | [delete_stmt の 2 番目と 3 番目](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13445-L13480) | B |

### CREATE TABLE

| 規則 | 採用する選択肢 | 外す選択肢 |
| --- | --- | --- |
| [create_table_stmt](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L3217-L3248) | 1 番目を `CREATE TABLE opt_if_not_exists table_ident '(' table_element_list ')'` に縮めたもの | `opt_temporary` (A)、`opt_create_table_options_etc` (下記)、2 番目 (要素の並びなし、B: `CREATE TABLE ... AS SELECT` 前提)、3 番目と 4 番目 (`LIKE`、B) |
| [opt_if_not_exists](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L6498-L6501) | 省略、`IF NOT EXISTS` | |
| [table_element_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L6766-L6779)、[table_element](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L6781-L6784) | `,` 区切りの `column_def` / `table_constraint_def` | |

- 列定義

| 規則 | 採用する選択肢 | 外す選択肢 |
| --- | --- | --- |
| [column_def](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L6786-L6791) | `ident field_def` | `opt_references` (D: 列定義内の `REFERENCES` は MySQL 自身が読み飛ばす) |
| [field_def](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L6868-L6892) | `type opt_column_attribute_list` | 生成列 (`[GENERATED ALWAYS] AS (expr) [VIRTUAL \| STORED]`、B) |
| [type](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L6905-L7113) | `varchar field_length` | `opt_charset_with_opt_binary` (B: 文字集合と照合順序)、その他のすべての型 (A) |
| [varchar](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7120-L7123) | `CHAR VARYING`、`VARCHAR` (字句解析で `VARCHARACTER` も `VARCHAR` になる) | |
| [field_length](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7238-L7242) | 4 つすべて (`'(' NUM ')'` と、大きな数のトークン `LONG_NUM` / `ULONGLONG_NUM` / `DECIMAL_NUM` の形) | |
| [opt_column_attribute_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7258-L7261)、[column_attribute_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7263-L7299) | 属性の並び (`[NOT] ENFORCED` の並びの検査は `CHECK` と一緒に外す) | |
| [column_attribute](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7301-L7401) | `NOT NULL`、`[PRIMARY] KEY`、`UNIQUE`、`UNIQUE KEY` | 下記 |

- 列定義で外した属性の一覧

| 属性 | 理由 |
| --- | --- |
| `NULL` | A (列は常に NOT NULL) |
| `NOT SECONDARY` | B (セカンダリエンジン) |
| `DEFAULT 値` / `DEFAULT (式)` | B (列の既定値) |
| `ON UPDATE NOW()` | B |
| `AUTO_INCREMENT` / `SERIAL DEFAULT VALUE` | B (自動採番) |
| `COMMENT '...'` | B |
| `COLLATE 照合順序` | B |
| `COLUMN_FORMAT` / `STORAGE` | B |
| `SRID` | B (空間データ) |
| `[CONSTRAINT [名前]] CHECK (式)` と `[NOT] ENFORCED` | B (検査制約) |
| `ENGINE_ATTRIBUTE` / `SECONDARY_ENGINE_ATTRIBUTE` | B |
| `VISIBLE` / `INVISIBLE` | B (不可視列) |
| 列定義内の `REFERENCES ...` | D |

- 列定義内の `REFERENCES` について
  - MySQL の文法は列定義の末尾に `REFERENCES` 句を受理するが、アクションは `nullptr` を返して捨てており、外部キーにはならない
  - MySQL のマニュアルも「MySQL parses but ignores "inline `REFERENCES` specifications" (as defined in the SQL standard) where the references are defined as part of the column specification. MySQL accepts `REFERENCES` clauses only when specified as part of a separate `FOREIGN KEY` specification.」と説明している
  - minesql では、書いても効かない構文を受理する意味がないため、構文エラーにする
  - 参照:
    - [opt_references (`Currently we ignore FK references here`)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L6793-L6800)
    - [CREATE TABLE Statement (MySQL 8.4 Reference Manual)](https://dev.mysql.com/doc/refman/8.4/en/create-table.html)

- テーブル制約 (インデックスと外部キー)

| 規則 | 採用する選択肢 | 外す選択肢 |
| --- | --- | --- |
| [table_constraint_def](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L6802-L6843) | 1 番目を `key_or_index opt_ident '(' key_list ')'` に、4 番目を `opt_constraint_name constraint_key_type opt_ident '(' key_list ')'` に縮めたもの、5 番目 (`opt_constraint_name FOREIGN KEY opt_ident '(' key_list ')' references`) | 2 番目 (`FULLTEXT`、B)、3 番目 (`SPATIAL`、B)、6 番目 (`CHECK`、B)、`opt_index_name_and_type` を `opt_ident` に置き換え (下記)、`key_list_with_expression` を `key_list` に置き換え (B: 関数インデックス)、`opt_index_options` (B) |
| [opt_constraint_name](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L6849-L6852) | 省略、`CONSTRAINT [ident]` | |
| [constraint_key_type](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7686-L7689) | `PRIMARY KEY`、`UNIQUE [KEY \| INDEX]` | |
| [key_or_index](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7691-L7694)、[opt_key_or_index](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7696-L7699) | `KEY` と `INDEX` (同義語) | |
| [opt_ident](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7924-L7927) | 省略、`ident` (インデックス名) | |
| [key_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7861-L7875) | `,` 区切りの `key_part` | |
| [key_part](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7877-L7896) | `ident` | `opt_ordering_direction` (B: 降順インデックス)、`ident '(' NUM ')'` (B: プレフィックスインデックス) |
| [references](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7604-L7617) | `REFERENCES table_ident opt_ref_list opt_on_update_delete` | `opt_match_clause` (D、下記) |
| [opt_ref_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7619-L7622)、[reference_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7624-L7639) | 省略、`'(' ident, ... ')'` | |
| [opt_on_update_delete](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7648-L7676) | 5 つすべて (省略、`ON UPDATE`、`ON DELETE`、両方の 2 通りの順序) | |
| [delete_option](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7678-L7684) | `RESTRICT`、`NO ACTION` | `CASCADE` (B)、`SET NULL` (A)、`SET DEFAULT` (B) |

- 外部キーについて
  - `NO ACTION` を `RESTRICT` と並べて採用するのは、MySQL のマニュアルが「`NO ACTION`: A keyword from standard SQL. For `InnoDB`, this is equivalent to `RESTRICT`; the delete or update operation for the parent table is immediately rejected if there is a related foreign key value in the referenced table.」と説明しているため
    - minesql の外部キーは `RESTRICT` だけを実装しており ([サポート状況](../../../feature/support-status.md))、`NO ACTION` は同じ動作の別表記として受理する
  - `MATCH FULL | PARTIAL | SIMPLE` を外すのは、MySQL がこの句をデータディクショナリに保存するだけで、どのストレージエンジンも参照しないため
    - マニュアルは「no storage engine, including `InnoDB`, recognizes or enforces the `MATCH` clause used in referential integrity constraint definitions. Use of an explicit `MATCH` clause does not have the specified effect, and also causes `ON DELETE` and `ON UPDATE` clauses to be ignored.」と説明している
  - 参照される列の並びの省略 (`REFERENCES parent` だけの形) と、複合外部キー (列が 2 つ以上) を受理するかは、文法では MySQL と同じ形を受理したうえでプリペアで決める
  - 参照:
    - [FOREIGN KEY Constraints (MySQL 8.4 Reference Manual)](https://dev.mysql.com/doc/refman/8.4/en/create-table-foreign-keys.html)
    - [dd_table.cc での MATCH の保存](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/dd/dd_table.cc#L1182-L1194)
    - [dict0dd.cc での参照アクションの読み取り (MATCH は読まない)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/storage/innobase/dict/dict0dd.cc#L4343-L4380)
- インデックス定義で外した構文の一覧

| 構文 | 規則 | 理由 |
| --- | --- | --- |
| `USING BTREE \| HASH` / `TYPE BTREE \| HASH` | [opt_index_name_and_type](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7834-L7838) | B (インデックスの種類は B+Tree だけ) |
| `KEY_BLOCK_SIZE` / `COMMENT` / `VISIBLE` / `INVISIBLE` / `ENGINE_ATTRIBUTE` | [opt_index_options](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7771-L7774) | B |
| 列の `ASC` / `DESC` | [key_part](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7877-L7896) | B |
| 列のプレフィックス長 `col(10)` | 同上 | B |
| 式によるキー `((expr))` | [key_part_with_expression](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L7914-L7922) | B |
| `FULLTEXT` / `SPATIAL` | [table_constraint_def](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L6802-L6843) | B |
| `CHECK (式)` | [check_constraint](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L6845-L6847) | B |

- テーブルオプションと後続の句で外したものの一覧

| 構文 | 規則 | 理由 |
| --- | --- | --- |
| `TEMPORARY` | [opt_temporary](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13031-L13034) | A |
| `ENGINE = ...`、`DEFAULT CHARSET = ...`、`COLLATE = ...`、`COMMENT = ...`、`AUTO_INCREMENT = ...`、`ROW_FORMAT = ...` など | [create_table_option](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L6538) | B (ストレージエンジンと文字集合は 1 つに固定で、指定を解釈する実装がない) |
| `PARTITION BY ...` | [opt_create_partitioning_etc](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L6081-L6088) | A |
| `[IGNORE \| REPLACE] [AS] SELECT ...` | [opt_duplicate_as_qe](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L6090) | B |
| `LIKE 元テーブル` / `(LIKE 元テーブル)` | [create_table_stmt の 3 番目と 4 番目](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L3217-L3248) | B |

### CREATE SCHEMA

| 規則 | 採用する選択肢 | 外す選択肢 |
| --- | --- | --- |
| [create](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L3269-L3284) | 1 番目を `CREATE DATABASE opt_if_not_exists ident` に縮めたもの (`SCHEMA` は字句解析で `DATABASE` と同じトークンになる) | `opt_create_database_options` (B: 文字集合・照合順序・暗号化の指定)、`create` 規則の他の選択肢 (ビュー、トリガー、ユーザー、ロールなど、B) |
| [opt_if_not_exists](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L6498-L6501) | 省略、`IF NOT EXISTS` | |

- MySQL の `create` 規則は `CREATE DATABASE opt_if_not_exists ident` の直後に規則の途中のアクションを持つ古い書き方で、minesql は末尾のアクションで AST ノードを作る
- 外した句と選択肢の一覧

| 句 / 選択肢 | 規則 | 理由 |
| --- | --- | --- |
| `DEFAULT CHARACTER SET` / `DEFAULT COLLATE` / `DEFAULT ENCRYPTION` | [opt_create_database_options](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L6463-L6471)、[create_database_option](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L6473) | B |

### DROP SCHEMA

| 規則 | 採用する選択肢 | 外す選択肢 |
| --- | --- | --- |
| [drop_database_stmt](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12762-L12771) | `DROP DATABASE if_exists ident` (`SCHEMA` は `DATABASE` と同じトークン) | |
| [if_exists](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13021-L13024) | 省略、`IF EXISTS` | |

### USE

| 規則 | 採用する選択肢 | 外す選択肢 |
| --- | --- | --- |
| [use](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L14368-L14379) | `USE ident` | |

- MySQL の `use` 規則のアクションはストアドプログラムの中での `USE` を拒否する検査を持つが、minesql にはストアドプログラムがないので写さない
- MySQL の X Plugin は `USE` の後に `CURRENT_SCHEMA` の Notice を送らない
  - `plugin/x/src` を `CURRENT_SCHEMA` で検索して送信箇所がなく、Notice の定義に値だけがある状態

### DROP TABLE

| 規則 | 採用する選択肢 | 外す選択肢 |
| --- | --- | --- |
| [drop_table_stmt](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12733-L12751) | `DROP table_or_tables if_exists table_list` | `opt_temporary` (A)、`opt_restrict` (D: アクションのコメントに `opt_restrict ($6) is ignored!` とあるとおり読み飛ばされる) |
| [table_or_tables](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L16163-L16166) | `TABLE`、`TABLES` (同義語) | |
| [if_exists](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L13021-L13024) | 省略、`IF EXISTS` | |
| [table_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L12991-L13004) | `,` 区切りの `table_ident` | |

- 参照:
  - [opt_restrict](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9041-L9045)

### トランザクション制御

| 規則 | 採用する選択肢 | 外す選択肢 |
| --- | --- | --- |
| [begin_stmt](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L17255-L17263) | `BEGIN opt_work` | |
| [start](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9175-L9189) | `START TRANSACTION opt_start_transaction_option_list` | |
| [commit](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L17287-L17297) | `COMMIT opt_work` | `opt_chain` / `opt_release` (B) |
| [rollback](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L17299-L17316) | `ROLLBACK opt_work` | `opt_chain` / `opt_release` (B)、`ROLLBACK [WORK] TO [SAVEPOINT] ident` (B: セーブポイント) |
| [opt_work](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L17265-L17268) | 省略、`WORK` | |
| [opt_start_transaction_option_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9191-L9200)、[start_transaction_option_list](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9202-L9211) | 省略、`,` 区切りの `start_transaction_option` | |
| [start_transaction_option](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9213-L9226) | `WITH CONSISTENT SNAPSHOT` | `READ ONLY` / `READ WRITE` (B) |

- MySQL の `begin_stmt` と `commit` / `rollback` は、parse tree を作らず規則のアクションで `LEX` に直接コマンドを書き込む古い書き方のまま残っている (`begin_stmt` は規則の途中にアクションがある)
  - minesql では他のステートメントと同じく、規則末尾のアクションで AST ノードを作る
- 外した句と選択肢の一覧

| 句 / 選択肢 | 規則 | 理由 |
| --- | --- | --- |
| `READ ONLY` / `READ WRITE` | [start_transaction_option](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L9213-L9226) | B |
| `AND [NO] CHAIN` | [opt_chain](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L17270-L17274) | B |
| `[NO] RELEASE` | [opt_release](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L17276-L17280) | B |
| `ROLLBACK TO SAVEPOINT` / `SAVEPOINT` / `RELEASE SAVEPOINT` | [rollback の 2 番目](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L17299-L17316)、[savepoint](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L17318)、[release](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L17327) | B |

## 構文エラー

- 形式は MySQL の `ER_PARSE_ERROR` (エラー番号 1064、SQLSTATE `42000`) に合わせる
  - 文言は `%s near '%-.80s' at line %d` で、`%s` には `ER_SYNTAX_ERROR` の文 (`You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use`) が入る
  - `near` の中身はエラー位置から入力の末尾までの文字列で、80 文字で切る
  - 行番号はエラー位置より前にある改行の数に 1 を足したもの
  - 参照:
    - [ER_PARSE_ERROR](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/share/messages_to_clients.txt#L1561-L1565)
    - [ER_SYNTAX_ERROR](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/share/messages_to_clients.txt#L3439-L3443)
    - [THD::vsyntax_error_at (文言と行番号の組み立て)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_class.cc#L2853-L2871)
    - [Lex_input_stream::get_lineno](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1193-L1207)
- エラー位置は、構文解析器がエラーを検出したときの先読みトークンの先頭
  - Bison は生成したパーサーからエラー報告関数 (`my_sql_parser_error`) を先読みトークンの位置つきで呼び、MySQL はその位置を `near` の起点にする
  - 入力の終わりでエラーになった場合 (`SELECT * FROM` のように途中で切れた入力) は `near ''` になる
  - goyacc の生成コードは `yyLexer.Error(string)` を呼ぶだけで位置を渡さないため、minesql の字句解析器は直前に返したトークンの先頭位置を覚えておき、`Error` の中でそれを使って同じ文言を組み立てる
  - 参照:
    - [my_sql_parser_error](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L334-L346)
    - [THD::syntax_error_at](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_class.cc#L2804-L2809)
    - [goyacc の yyLexer インターフェース (doc.go)](https://github.com/golang/tools/blob/v0.31.0/cmd/goyacc/doc.go#L27-L36)
- 字句の誤りも同じ構文エラーとして報告する
  - MySQL では、閉じていない文字列は引用符 1 文字のトークンとして、閉じていないバッククォートと閉じていないコメントは受理不能なトークン (`ABORT_SYM`) として構文解析器に渡り、そこで構文エラーになる
  - minesql も字句解析器ではエラーを起こさず、受理不能なトークンを返して構文解析器に判断させる (エラーの出口を 1 つにするため)
  - 参照:
    - [閉じていない文字列 (get_text が失敗したら 1 文字ずつ読む)](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1861-L1865)
    - [閉じていないバッククォート](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1736-L1739)
    - [閉じていないコメント](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_lex.cc#L1994)
- 空の入力は `ER_EMPTY_QUERY` (エラー番号 1065、SQLSTATE `42000`、文言 `Query was empty`) を返す
  - 参照:
    - [ER_EMPTY_QUERY](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/share/messages_to_clients.txt#L1585-L1589)
    - [sql_statement での判定](https://github.com/mysql/mysql-server/blob/aa461240270d809bcac336483b886b3d1789d4d9/sql/sql_yacc.yy#L2348-L2358)
- 期待していたトークンの列挙 (goyacc の `yyErrorVerbose`) は文言に含めず、開発時の診断にだけ使う ([ideology.md](../../ideology.md) の判断)
- 構文エラーの深刻度は `ERROR` で、セッションは継続する ([sql_parser.md のエラー](../sql_parser.md#エラー))
- 名前の妥当性 (長さ、末尾の空白、存在) の誤りは構文エラーではなく、プリペアのエラーになる
