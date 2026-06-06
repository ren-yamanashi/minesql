package access

import (
	"errors"
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// resolvePrevVersionInput は resolvePrevVersion の引数
type resolvePrevVersionInput struct {
	mtr        *buffer.Mtr
	undoLog    *undo.Manager
	catalog    *dictionary.Catalog
	bufferPool *buffer.Pool
	fileId     page.FileId
	record     *PrimaryRecord
}

// resolvePrevVersion は record の rollPtr を辿って 1 つ前のバージョンを再構築する
//   - チェーン終端 (rollPtr が Null / Insert Undo に到達) の場合は nil を返す
func resolvePrevVersion(in resolvePrevVersionInput) (*PrimaryRecord, error) {
	if in.record.rollPtr.IsNull() {
		return nil, nil //nolint:nilnil // nil は「チェーン終端」を表す
	}

	undoRecord, err := in.undoLog.LookupByPointer(in.mtr, in.record.rollPtr)
	if err != nil {
		if errors.Is(err, undo.ErrNullPointer) {
			return nil, nil //nolint:nilnil // nil は「チェーン終端」を表す
		}
		return nil, err
	}

	var oldRow btree.Record
	switch undoRecord.RecordType() {
	case undo.RecordTypeInsert:
		return nil, nil //nolint:nilnil // nil は「チェーン終端」を表す
	case undo.RecordTypeUpdate:
		ur, ok := undoRecord.(undo.UpdateRecord)
		if !ok {
			return nil, fmt.Errorf("access: undo record type mismatch (expected UpdateRecord)")
		}
		oldRow = ur.PrevRecord()
	case undo.RecordTypeDelete:
		dr, ok := undoRecord.(undo.DeleteRecord)
		if !ok {
			return nil, fmt.Errorf("access: undo record type mismatch (expected DeleteRecord)")
		}
		oldRow = dr.Record()
	default:
		return nil, fmt.Errorf("access: unknown undo record type %v", undoRecord.RecordType())
	}

	prevRecord, err := DecodePrimaryRecord(oldRow, in.catalog, in.bufferPool, in.fileId)
	if err != nil {
		return nil, err
	}
	prevRecord.lastTrxId = undoRecord.PrevLastTrxId()
	prevRecord.rollPtr = undoRecord.PrevRollPtr()
	prevRecord.deleteMark = 0
	return prevRecord, nil
}
