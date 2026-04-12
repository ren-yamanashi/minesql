package access

import (
	"minesql/internal/storage/btree"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/encode"
)

type TableIterator struct {
	iterator      *btree.Iterator
	bp            *buffer.BufferPool
	rv            *ReadView
	versionReader *VersionReader
}

func newTableIterator(iterator *btree.Iterator, bp *buffer.BufferPool, rv *ReadView, vr *VersionReader) *TableIterator {
	return &TableIterator{
		iterator:      iterator,
		bp:            bp,
		rv:            rv,
		versionReader: vr,
	}
}

// Next はデコード済みの次の可視レコードを返す
//
// ReadView に基づいて可視性を判定し、不可視なレコードは undo チェーンを辿って旧バージョンを探す。
// 可視かつ DeleteMark が設定されているレコードはスキップする。ロックは取得しない。
//
// 戻り値: レコード (プライマリキー + 値), データがあるかどうか, エラー
func (ri *TableIterator) Next() ([][]byte, bool, error) {
	for {
		btrRecord, ok, err := ri.iterator.Next(ri.bp)
		if !ok {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}

		// B+Tree レコードから lastModified, rollPtr を取り出す
		deleteMark := btrRecord.HeaderBytes()[0]
		lastModified, rollPtr, nonKeyColumns := decodeRecordNonKey(btrRecord.NonKeyBytes())

		// カラムデータをデコード
		var columns [][]byte
		encode.Decode(btrRecord.KeyBytes(), &columns)
		encode.Decode(nonKeyColumns, &columns)

		// 可視なバージョンを探す
		current := RecordVersion{
			LastModified: lastModified,
			RollPtr:      rollPtr,
			DeleteMark:   deleteMark,
			Columns:      columns,
		}
		visible, found, err := ri.versionReader.ReadVisibleVersion(ri.rv, current)
		if err != nil {
			return nil, false, err
		}
		if !found {
			continue
		}

		// 可視だが DeleteMark が設定されている場合はスキップ
		if visible.DeleteMark == 1 {
			continue
		}

		return visible.Columns, true, nil
	}
}
