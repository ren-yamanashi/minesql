package access

import (
	"encoding/binary"
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// Table はテーブルへのアクセスを提供する
type Table struct {
	primaryIndex     *primaryIndex
	secondaryIndexes []*secondaryIndex
	catalog          *catalog.Catalog
	undoLog          *undo.Manager
	lock             *lock.Manager
	bufferPool       *buffer.BufferPool
}

// NewTable は既存のテーブルを開く
func NewTable(bp *buffer.BufferPool, ct *catalog.Catalog, undo *undo.Manager, lock *lock.Manager, name string) (*Table, error) {
	table, err := fetchTable(ct, name)
	if err != nil {
		return nil, err
	}

	fileId := table.MetaPageId.FileId
	pi, err := fetchPrimaryIndex(ct, bp, fileId, lock)
	if err != nil {
		return nil, err
	}

	sis, err := fetchSecondaryIndexes(ct, bp, fileId, pi.tree, lock)
	if err != nil {
		return nil, err
	}

	return &Table{
		primaryIndex:     pi,
		secondaryIndexes: sis,
		catalog:          ct,
		undoLog:          undo,
		lock:             lock,
		bufferPool:       bp,
	}, nil
}

// fetchTable はテーブル名から TableRecord を取得する
func fetchTable(ct *catalog.Catalog, name string) (catalog.TableRecord, error) {
	iter, err := ct.TableMeta.Search(catalog.SearchModeKey{Key: [][]byte{[]byte(name)}})
	if err != nil {
		return catalog.TableRecord{}, err
	}
	record, ok, err := iter.Next()
	if err != nil {
		return catalog.TableRecord{}, err
	}
	if !ok || record.Name != name {
		return catalog.TableRecord{}, fmt.Errorf("table %q not found", name)
	}
	return record, nil
}

// fetchPrimaryIndex はカタログからプライマリインデックスを取得して PrimaryIndex を構築する
func fetchPrimaryIndex(ct *catalog.Catalog, bp *buffer.BufferPool, fileId page.FileId, lock *lock.Manager) (*primaryIndex, error) {
	record, err := fetchPrimaryIndexRecord(ct, fileId)
	if err != nil {
		return nil, err
	}
	return newPrimaryIndex(ct, bp, record.MetaPageId, record.NumOfCol, lock), nil
}

// fetchPrimaryIndexRecord はカタログからプライマリインデックスの IndexRecord を取得する
func fetchPrimaryIndexRecord(ct *catalog.Catalog, fileId page.FileId) (catalog.IndexRecord, error) {
	fileIdBytes := binary.BigEndian.AppendUint32(nil, uint32(fileId))
	key := catalog.SearchModeKey{
		Key: [][]byte{fileIdBytes, []byte(catalog.PrimaryIndexName)},
	}
	iter, err := ct.IndexMeta.Search(key)
	if err != nil {
		return catalog.IndexRecord{}, err
	}
	record, ok, err := iter.Next()
	if err != nil {
		return catalog.IndexRecord{}, err
	}
	if !ok || record.FileId != fileId || record.Name != catalog.PrimaryIndexName {
		return catalog.IndexRecord{}, fmt.Errorf("primary index not found for table (file %d)", fileId)
	}
	return record, nil
}

// fetchSecondaryIndexes は指定テーブルのセカンダリインデックス一覧を返す
func fetchSecondaryIndexes(
	ct *catalog.Catalog,
	bp *buffer.BufferPool,
	fileId page.FileId,
	pt *btree.Btree,
	lock *lock.Manager,
) ([]*secondaryIndex, error) {
	records, err := fetchSecondaryIndexRecords(ct, fileId)
	if err != nil {
		return nil, err
	}
	indexes := make([]*secondaryIndex, 0, len(records))
	for _, record := range records {
		index := newSecondaryIndex(ct, bp, newSecondaryIndexInput{
			MetaPageId:  record.MetaPageId,
			PrimaryTree: pt,
			IndexId:     record.IndexId,
			IndexName:   record.Name,
			Unique:      record.IndexType == catalog.IndexTypeUnique,
			Lock:        lock,
		})
		indexes = append(indexes, index)
	}
	return indexes, nil
}

// fetchSecondaryIndexRecords はカタログからセカンダリインデックスの IndexRecord 一覧を取得する
func fetchSecondaryIndexRecords(ct *catalog.Catalog, fileId page.FileId) ([]catalog.IndexRecord, error) {
	fileIdBytes := binary.BigEndian.AppendUint32(nil, uint32(fileId))
	iter, err := ct.IndexMeta.Search(catalog.SearchModeKey{Key: [][]byte{fileIdBytes}})
	if err != nil {
		return nil, err
	}
	var records []catalog.IndexRecord
	for {
		record, ok, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if !ok || record.FileId != fileId {
			break
		}
		if record.Name == catalog.PrimaryIndexName {
			continue
		}
		records = append(records, record)
	}
	return records, nil
}

// buildValMap はカラム名 → 値のマップを構築する
func (t *Table) buildValMap(colNames, values []string) map[string]string {
	m := make(map[string]string, len(colNames))
	for i, name := range colNames {
		m[name] = values[i]
	}
	return m
}

// extractPrimaryKey はテーブル定義順のカラム値からプライマリキー部分を抽出する
func (t *Table) extractPrimaryKey(values []string) []string {
	pk := make([]string, t.primaryIndex.pkCount)
	copy(pk, values[:t.primaryIndex.pkCount])
	return pk
}

// extractSecondaryKey は keyCols と valMap からインデックス定義順のセカンダリキーのカラム名とカラム値を抽出する
func (t *Table) extractSecondaryKey(keyCols map[string]int, valMap map[string]string) (colNames, values []string) {
	colNames = make([]string, len(keyCols))
	values = make([]string, len(keyCols))
	for name, pos := range keyCols {
		colNames[pos] = name
		values[pos] = valMap[name]
	}
	return colNames, values
}

// buildSecondaryRecord はセカンダリインデックス用のレコードを構築する
func (t *Table) buildSecondaryRecord(si *secondaryIndex, skColNames, skValues, pk []string) (*secondaryRecord, error) {
	return newSecondaryRecord(t.catalog, newSecondaryRecordInput{
		fileId:     si.fileId,
		deleteMark: 0,
		indexName:  si.indexName,
		colNames:   skColNames,
		values:     skValues,
		pk:         pk,
	})
}
