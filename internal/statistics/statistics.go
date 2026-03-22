package statistics

import (
	"bytes"
	"minesql/internal/access"
	"minesql/internal/catalog"
	"minesql/internal/executor"
	"minesql/internal/storage/bufferpool"
)

type ColumnStatistics struct {
	UniqueValues uint64 // カラムの異なる値の数 = V(T, F)
	MinValue     []byte // カラムの最小値 = min(F)
	MaxValue     []byte // カラムの最大値 = max(F)
}

type IndexStatistics struct {
	Height uint64 // B+Tree の高さ (ルートからリーフまでのページ数) = H(T)
}

type TableStatistics struct {
	RecordCount         uint64                      // テーブルのレコード数 = R(T)
	LeafPageCount       uint64                      // テーブルのリーフページ数 (=リーフノード数) = P(T)
	ColumnStats         map[string]ColumnStatistics // カラム名 -> カラム統計情報
	SecondaryIndexStats map[string]IndexStatistics  // セカンダリインデックス名 -> インデックス統計情報
}

// Statistics はテーブルの統計情報を収集する
type Statistics struct {
	metadata   *catalog.TableMetadata
	bufferPool *bufferpool.BufferPool
}

func NewStatistics(meta *catalog.TableMetadata, bp *bufferpool.BufferPool) *Statistics {
	return &Statistics{
		metadata:   meta,
		bufferPool: bp,
	}
}

// Analyze はテーブルごとの統計情報を収集する
func (s *Statistics) Analyze() (TableStatistics, error) {
	tbl, err := s.metadata.GetTable()
	if err != nil {
		return TableStatistics{}, err
	}

	// テーブルのレコード数を取得
	tableStats, err := s.analyzeTable(tbl)
	if err != nil {
		return TableStatistics{}, err
	}
	return *tableStats, nil
}

// analyzeTable はテーブルおよびそのテーブル内の全カラムの統計情報を収集する
func (s *Statistics) analyzeTable(table *access.TableAccessMethod) (*TableStatistics, error) {
	scan := executor.NewTableScan(
		table,
		access.RecordSearchModeStart{},
		func(record executor.Record) bool {
			return true
		},
	)

	var recordCount uint64
	columnStats := make(map[string]ColumnStatistics)
	uniqueValues := make(map[string]map[string]struct{}) // カラム名 -> 値の Set
	colMetadata := s.metadata.GetSortedCols()            // カラムの位置 (Pos) でソートされたカラムメタデータ

	// テーブルをスキャンして統計情報を収集
	for {
		record, err := scan.Next()
		if err != nil {
			return nil, err
		}
		if record == nil {
			break
		}
		recordCount++
		updateColumnStats(colMetadata, record, recordCount, columnStats, uniqueValues)
	}

	// uniqueValues の Set のサイズから V(T, F) を算出
	for colName, values := range uniqueValues {
		colStat := columnStats[colName]
		colStat.UniqueValues = uint64(len(values))
		columnStats[colName] = colStat
	}

	// テーブルのリーフページ数を取得
	leafPageCount, err := table.LeafPageCount(s.bufferPool)
	if err != nil {
		return nil, err
	}

	// ユニークインデックスの統計情報を収集
	secondaryIndexStats, err := s.analyzeIndex(table)
	if err != nil {
		return nil, err
	}

	return &TableStatistics{
		RecordCount:         recordCount,
		LeafPageCount:       leafPageCount,
		ColumnStats:         columnStats,
		SecondaryIndexStats: secondaryIndexStats,
	}, nil
}

// updateColumnStats は 1 レコード分の各カラムの統計情報 (uniqueValues, min, max) を更新する
//
// colMetadata: カラムの位置 (Pos) でソートされたカラムメタデータ
//
// record: レコードの各カラム値 (colMetadata と同じ順序)
//
// recordCount: 現在のレコード番号 (1 始まり。1 のとき min/max を初期化する)
//
// columnStats: カラム名 -> カラム統計情報 (結果が代入される)
//
// uniqueValues: カラム名 -> 値の Set (結果が代入される)
func updateColumnStats(
	colMetadata []*catalog.ColumnMetadata,
	record executor.Record,
	recordCount uint64,
	columnStats map[string]ColumnStatistics,
	uniqueValues map[string]map[string]struct{},
) {
	for i, colMeta := range colMetadata {
		colName := colMeta.Name
		colValue := record[i]
		colStat := columnStats[colName]

		// uniqueValues の Set に追加
		if uniqueValues[colName] == nil {
			uniqueValues[colName] = make(map[string]struct{})
		}
		uniqueValues[colName][string(colValue)] = struct{}{}

		// min/max の更新
		if recordCount == 1 {
			colStat.MinValue = colValue
			colStat.MaxValue = colValue
		} else {
			if bytes.Compare(colValue, colStat.MinValue) < 0 {
				colStat.MinValue = colValue
			}
			if bytes.Compare(colValue, colStat.MaxValue) > 0 {
				colStat.MaxValue = colValue
			}
		}

		columnStats[colName] = colStat
	}
}

func (s *Statistics) analyzeIndex(tbl *access.TableAccessMethod) (map[string]IndexStatistics, error) {
	secondaryIndexStats := make(map[string]IndexStatistics)
	for _, uIdx := range tbl.UniqueIndexes {
		height, err := uIdx.Height(s.bufferPool)
		if err != nil {
			return nil, err
		}
		secondaryIndexStats[uIdx.Name] = IndexStatistics{Height: height}
	}
	return secondaryIndexStats, nil
}
