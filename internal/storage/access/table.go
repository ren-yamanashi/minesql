package access

import (
	"encoding/binary"
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// Table はテーブルへのアクセスを提供する
type Table struct {
	primaryIndex     *primaryIndex
	secondaryIndexes []*secondaryIndex
	catalog          *dictionary.Catalog
	undoLog          *undo.Manager
	lock             *lock.Manager
	bufferPool       *buffer.Pool
	redoLog          *redo.Buffer
}

// NewTable は既存のテーブルを開く
func NewTable(
	bp *buffer.Pool,
	ct *dictionary.Catalog,
	undo *undo.Manager,
	lock *lock.Manager,
	redoLog *redo.Buffer,
	name string,
) (*Table, error) {
	table, err := fetchTable(ct, bp, name)
	if err != nil {
		return nil, err
	}

	fileId := table.MetaPageId().FileId()
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
		redoLog:          redoLog,
	}, nil
}

// fetchTable はテーブル名から TableRecord を取得する
func fetchTable(ct *dictionary.Catalog, bp *buffer.Pool, name string) (dictionary.TableMetaRecord, error) {
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	iter, err := ct.TableMeta().Search(mtr, dictionary.SearchModeKey{Key: [][]byte{[]byte(name)}})
	if err != nil {
		return dictionary.TableMetaRecord{}, err
	}
	defer iter.Close()
	record, ok, err := iter.Next()
	if err != nil {
		return dictionary.TableMetaRecord{}, err
	}
	if !ok || record.Name() != name {
		return dictionary.TableMetaRecord{}, fmt.Errorf("table %q not found", name)
	}
	return record, nil
}

// fetchPrimaryIndex はカタログからプライマリインデックスを取得して PrimaryIndex を構築する
func fetchPrimaryIndex(
	ct *dictionary.Catalog,
	bp *buffer.Pool,
	fileId page.FileId,
	lock *lock.Manager,
) (*primaryIndex, error) {
	record, err := fetchPrimaryIndexRecord(ct, bp, fileId)
	if err != nil {
		return nil, err
	}
	return newPrimaryIndex(ct, bp, record.MetaPageId(), record.ColumnCount(), lock), nil
}

// fetchPrimaryIndexRecord はカタログからプライマリインデックスの IndexRecord を取得する
func fetchPrimaryIndexRecord(ct *dictionary.Catalog, bp *buffer.Pool, fileId page.FileId) (dictionary.IndexMetaRecord, error) {
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	fileIdBytes := binary.BigEndian.AppendUint32(nil, uint32(fileId))
	key := dictionary.SearchModeKey{
		Key: [][]byte{fileIdBytes, []byte(dictionary.PrimaryIndexName)},
	}
	iter, err := ct.IndexMeta().Search(mtr, key)
	if err != nil {
		return dictionary.IndexMetaRecord{}, err
	}
	defer iter.Close()
	record, ok, err := iter.Next()
	if err != nil {
		return dictionary.IndexMetaRecord{}, err
	}
	if !ok || record.FileId() != fileId || record.Name() != dictionary.PrimaryIndexName {
		return dictionary.IndexMetaRecord{}, fmt.Errorf("primary index not found for table (file %d)", fileId)
	}
	return record, nil
}

// fetchSecondaryIndexes は指定テーブルのセカンダリインデックス一覧を返す
func fetchSecondaryIndexes(
	ct *dictionary.Catalog,
	bp *buffer.Pool,
	fileId page.FileId,
	pt *btree.Tree,
	lock *lock.Manager,
) ([]*secondaryIndex, error) {
	records, err := fetchSecondaryIndexRecords(ct, bp, fileId)
	if err != nil {
		return nil, err
	}
	indexes := make([]*secondaryIndex, 0, len(records))
	for _, record := range records {
		index := newSecondaryIndex(ct, bp, newSecondaryIndexInput{
			MetaPageId:  record.MetaPageId(),
			PrimaryTree: pt,
			IndexId:     record.IndexId(),
			IndexName:   record.Name(),
			Unique:      record.IndexType() == dictionary.IndexTypeUnique,
			Lock:        lock,
		})
		indexes = append(indexes, index)
	}
	return indexes, nil
}

// fetchSecondaryIndexRecords はカタログからセカンダリインデックスの IndexRecord 一覧を取得する
func fetchSecondaryIndexRecords(ct *dictionary.Catalog, bp *buffer.Pool, fileId page.FileId) ([]dictionary.IndexMetaRecord, error) {
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	fileIdBytes := binary.BigEndian.AppendUint32(nil, uint32(fileId))
	iter, err := ct.IndexMeta().Search(mtr, dictionary.SearchModeKey{Key: [][]byte{fileIdBytes}})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var records []dictionary.IndexMetaRecord
	for {
		record, ok, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if !ok || record.FileId() != fileId {
			break
		}
		if record.Name() == dictionary.PrimaryIndexName {
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

// isPrimaryKeyColumn は指定したカラム名がプライマリキーのカラムかどうかを返す
func (t *Table) isPrimaryKeyColumn(colName string) (bool, error) {
	mtr := buffer.NewMtr(t.bufferPool)
	defer mtr.UnpinAll()
	fileId := t.primaryIndex.tree.MetaPageId().FileId()
	fileIdBytes := binary.BigEndian.AppendUint32(nil, uint32(fileId))
	iter, err := t.catalog.ColumnMeta().Search(mtr, dictionary.SearchModeKey{
		Key: [][]byte{fileIdBytes, []byte(colName)},
	})
	if err != nil {
		return false, err
	}
	defer iter.Close()

	col, ok, err := iter.Next()
	if err != nil {
		return false, err
	}
	if !ok || col.FileId() != fileId || col.Name() != colName {
		return false, nil
	}
	return col.Position() < t.primaryIndex.pkCount, nil
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
func (t *Table) buildSecondaryRecord(si *secondaryIndex, skColNames, skValues, pk []string) (*SecondaryRecord, error) {
	return NewSecondaryRecord(t.catalog, t.bufferPool, NewSecondaryRecordInput{
		fileId:     si.fileId,
		deleteMark: 0,
		indexName:  si.indexName,
		colNames:   skColNames,
		values:     skValues,
		pk:         pk,
	})
}
