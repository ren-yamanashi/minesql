package planner

import (
	"bytes"
	"minesql/internal/access"
	"minesql/internal/catalog"
	"minesql/internal/executor"
	"minesql/internal/storage/bufferpool"
)

type columnStatistics struct {
	uniqueValues uint64 // カラムの異なる値の数 = V(T, F)
	minValue     []byte // カラムの最小値 = min(F)
	maxValue     []byte // カラムの最大値 = max(F)
}

type indexStatistics struct {
	height uint64 // B+Tree の高さ (ルートからリーフまでのページ数) = H(T)
}

type tableStatistics struct {
	recordCount         uint64                      // テーブルのレコード数 = R(T)
	leafPageCount       uint64                      // テーブルのリーフページ数 (=リーフノード数) = P(T)
	columnStats         map[string]columnStatistics // カラム名 -> カラム統計情報
	secondaryIndexStats map[string]indexStatistics  // セカンダリインデックス名 -> インデックス統計情報
}

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
func (s *Statistics) Analyze() (tableStatistics, error) {
	tbl, err := s.metadata.GetTable()
	if err != nil {
		return tableStatistics{}, err
	}

	// テーブルのレコード数を取得
	tableStats, err := s.analyzeTable(tbl)
	if err != nil {
		return tableStatistics{}, err
	}
	return *tableStats, nil
}

// analyzeTable はテーブルおよびそのテーブル内の全カラムの統計情報を収集する
func (s *Statistics) analyzeTable(table *access.TableAccessMethod) (*tableStatistics, error) {
	scan := executor.NewTableScan(
		table,
		access.RecordSearchModeStart{},
		func(record executor.Record) bool {
			return true
		},
	)

	var recordCount uint64
	columnStats := make(map[string]columnStatistics)
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
		colStat.uniqueValues = uint64(len(values))
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

	return &tableStatistics{
		recordCount:         recordCount,
		leafPageCount:       leafPageCount,
		columnStats:         columnStats,
		secondaryIndexStats: secondaryIndexStats,
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
	columnStats map[string]columnStatistics,
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
			colStat.minValue = colValue
			colStat.maxValue = colValue
		} else {
			if bytes.Compare(colValue, colStat.minValue) < 0 {
				colStat.minValue = colValue
			}
			if bytes.Compare(colValue, colStat.maxValue) > 0 {
				colStat.maxValue = colValue
			}
		}

		columnStats[colName] = colStat
	}
}

func (s *Statistics) analyzeIndex(tbl *access.TableAccessMethod) (map[string]indexStatistics, error) {
	secondaryIndexStats := make(map[string]indexStatistics)
	for _, uIdx := range tbl.UniqueIndexes {
		height, err := uIdx.Height(s.bufferPool)
		if err != nil {
			return nil, err
		}
		secondaryIndexStats[uIdx.Name] = indexStatistics{height: height}
	}
	return secondaryIndexStats, nil
}
