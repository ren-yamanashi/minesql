package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// createSecondaryIndexes はセカンダリインデックスを作成する
//   - 各セカンダリインデックスごとに 1 mtr で「B+Tree 作成 + CreateBTreeUndo + IndexMeta Insert + MetaInsertUndo + 各 IndexKeyColumnMeta Insert + MetaInsertUndo」を原子的に記録する
//   - 途中失敗時はその mtr を UnpinAll で破棄するため、 そのセカンダリの中間状態はメモリ上から消える (既に Commit 済みのセカンダリは DDL Undo 経由で取り消す)
func createSecondaryIndexes(
	ct *dictionary.Catalog,
	bp *buffer.Pool,
	fileId page.FileId,
	pt *btree.Tree,
	lockMgr *lock.Manager,
	undoLog *undo.Manager,
	redoLog *redo.Buffer,
	inputs []CreateIndexInput,
) ([]*secondaryIndex, error) {
	indexes := make([]*secondaryIndex, 0, len(inputs))
	for _, input := range inputs {
		index, err := buildOneSecondaryIndex(ct, bp, fileId, pt, lockMgr, undoLog, redoLog, input)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, index)
	}
	return indexes, nil
}

// buildOneSecondaryIndex はセカンダリインデックス 1 件を 1 mtr で作成する
//   - B+Tree 作成、 CreateBTreeUndo、 IndexMeta Insert + MetaInsertUndo、 各 IndexKeyColumnMeta Insert + MetaInsertUndo を全て同一 mtr で行う
func buildOneSecondaryIndex(
	ct *dictionary.Catalog,
	bp *buffer.Pool,
	fileId page.FileId,
	pt *btree.Tree,
	lockMgr *lock.Manager,
	undoLog *undo.Manager,
	redoLog *redo.Buffer,
	input CreateIndexInput,
) (*secondaryIndex, error) {
	mtr := buffer.NewWriteMtr(bp, lock.DDLReservedTrxId, redoLog)

	indexId, err := ct.AllocateIndexId(mtr)
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}

	index, err := createSecondaryIndex(mtr, ct, bp, createSecondaryIndexInput{
		FileId:      fileId,
		PrimaryTree: pt,
		IndexId:     indexId,
		IndexName:   input.IndexName,
		Unique:      input.IndexType == dictionary.IndexTypeUnique,
		Lock:        lockMgr,
		UndoLog:     undoLog,
	})
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}

	createBTreeUndo := undo.NewDDLRecord(
		undo.DDLRecordTypeCreateBTree,
		undo.NewCreateBTreeUndoRecord(index.tree.MetaPageId()).Serialize(),
	)
	if err := ct.DDLManager().Append(mtr, createBTreeUndo); err != nil {
		mtr.UnpinAll()
		return nil, err
	}

	indexRecord := dictionary.NewIndexMetaRecord(
		fileId,
		indexId,
		input.IndexName,
		input.IndexType,
		len(input.ColNames),
		index.tree.MetaPageId(),
	)
	indexKey := indexRecord.Encode().Key()
	if err := ct.IndexMeta().Insert(mtr, indexRecord); err != nil {
		mtr.UnpinAll()
		return nil, err
	}
	if err := appendMetaInsertUndo(mtr, ct, undo.MetaTableTypeIndex, indexKey); err != nil {
		mtr.UnpinAll()
		return nil, err
	}

	for i, keyCol := range input.ColNames {
		keyColRecord := dictionary.NewIndexKeyColumnMetaRecord(indexId, keyCol, i)
		keyColKey := keyColRecord.Encode().Key()
		if err := ct.IndexKeyColumnMeta().Insert(mtr, keyColRecord); err != nil {
			mtr.UnpinAll()
			return nil, err
		}
		if err := appendMetaInsertUndo(mtr, ct, undo.MetaTableTypeIndexKeyColumn, keyColKey); err != nil {
			mtr.UnpinAll()
			return nil, err
		}
	}

	if err := mtr.Commit(); err != nil {
		return nil, err
	}
	return index, nil
}
