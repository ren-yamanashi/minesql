package dictionary

type ColumnStats struct {
	UniqueValues uint64 // カラムの異なる値の数 = V(T, F)
	MinValue     []byte // カラムの最小値 = min(F)
	MaxValue     []byte // カラムの最大値 = max(F)
}

type IndexStats struct {
	Height        uint64 // B+Tree の高さ (ルートからリーフまでのページ数) = H(I)
	LeafPageCount uint64 // B+Tree のリーフページ数 = Bl(I)
}

// TableStats はテーブルの統計情報を表す
//
// 参考: https://dev.mysql.com/doc/refman/8.0/ja/information-schema-innodb-tablestats-table.html
type TableStats struct {
	RecordCount   uint64                 // テーブルのレコード数 = R(T)
	LeafPageCount uint64                 // テーブルのリーフページ数 (=リーフノード数) = B(T)
	TreeHeight    uint64                 // プライマリキー B+Tree の高さ = H(T)
	ColStats      map[string]ColumnStats // カラム名 -> カラム統計情報
	IdxStats      map[string]IndexStats  // インデックス名 -> インデックス統計情報
}
