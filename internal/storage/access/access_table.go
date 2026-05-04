package access

import (
	"encoding/binary"
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// Table はテーブルへのアクセスを提供する
type Table struct {
	table            catalog.TableRecord
	primaryIndex     *PrimaryIndex
	secondaryIndexes []*SecondaryIndex
	catalog          *catalog.Catalog
	bufferPool       *buffer.BufferPool
}

// NewTable は既存のテーブルを開く
func NewTable(bp *buffer.BufferPool, ct *catalog.Catalog, name string) (*Table, error) {
	table, err := fetchTable(ct, name)
	if err != nil {
		return nil, err
	}

	fileId := table.MetaPageId.FileId
	pi, err := fetchPrimaryIndex(ct, bp, fileId)
	if err != nil {
		return nil, err
	}

	sis, err := fetchSecondaryIndexes(ct, bp, fileId, pi.tree)
	if err != nil {
		return nil, err
	}

	return &Table{
		table:            table,
		primaryIndex:     pi,
		secondaryIndexes: sis,
		catalog:          ct,
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
func fetchPrimaryIndex(ct *catalog.Catalog, bp *buffer.BufferPool, fileId page.FileId) (*PrimaryIndex, error) {
	fileIdBytes := binary.BigEndian.AppendUint32(nil, uint32(fileId))
	iter, err := ct.IndexMeta.Search(catalog.SearchModeKey{Key: [][]byte{fileIdBytes, []byte(catalog.PrimaryIndexName)}})
	if err != nil {
		return nil, err
	}
	record, ok, err := iter.Next()
	if err != nil {
		return nil, err
	}
	if !ok || record.FileId != fileId || record.Name != catalog.PrimaryIndexName {
		return nil, fmt.Errorf("primary index not found for table (file %d)", fileId)
	}
	return NewPrimaryIndex(ct, bp, record.MetaPageId, record.NumOfCol), nil
}

// fetchSecondaryIndexes は指定テーブルのセカンダリインデックス一覧を返す
func fetchSecondaryIndexes(
	ct *catalog.Catalog,
	bp *buffer.BufferPool,
	fileId page.FileId,
	pt *btree.Btree,
) ([]*SecondaryIndex, error) {
	fileIdBytes := binary.BigEndian.AppendUint32(nil, uint32(fileId))
	iter, err := ct.IndexMeta.Search(catalog.SearchModeKey{Key: [][]byte{fileIdBytes}})
	if err != nil {
		return nil, err
	}

	var indexes []*SecondaryIndex
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
		index := NewSecondaryIndex(ct, bp, NewSecondaryIndexInput{
			MetaPageId:  record.MetaPageId,
			PrimaryTree: pt,
			IndexId:     record.IndexId,
			IndexName:   record.Name,
			Unique:      record.IndexType == catalog.IndexTypeUnique,
		})
		indexes = append(indexes, index)
	}
	return indexes, nil
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
func (t *Table) buildSecondaryRecord(si *SecondaryIndex, skColNames, skValues, pk []string) (*SecondaryRecord, error) {
	return newSecondaryRecord(t.catalog, newSecondaryRecordInput{
		fileId:     si.fileId,
		deleteMark: 0,
		indexName:  si.indexName,
		colNames:   skColNames,
		values:     skValues,
		pk:         pk,
	})
}
