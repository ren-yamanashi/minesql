package access

import (
	"errors"
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// DDLRollbacker は DDL Undo レコードを物理基盤を経由して取り消す
//   - bufferPool: ページ解放・物理ファイル削除のために共有する
//   - catalog: Meta テーブル削除のために共有する
type DDLRollbacker struct {
	bufferPool *buffer.Pool
	catalog    *dictionary.Catalog
}

func NewDDLRollbacker(bp *buffer.Pool, catalog *dictionary.Catalog) *DDLRollbacker {
	return &DDLRollbacker{bufferPool: bp, catalog: catalog}
}

// Rollback は DDL Undo レコードの取り消しを 1 単位進める
//   - 取り消しが完了した場合は true を返す。CreateBTree 以外のレコードは常に 1 回で完了する
//   - CreateBTree は B+Tree の step 解放 1 単位を進めるため、呼び出し側は true が返るまで
//     新しい mtr で繰り返す
func (r *DDLRollbacker) Rollback(mtr *buffer.Mtr, record undo.DDLRecord) (bool, error) {
	switch record.RecordType() {
	case undo.DDLRecordTypeCreateBTree:
		return r.rollbackCreateBTree(mtr, record.Payload())
	case undo.DDLRecordTypeMetaInsert:
		return true, r.rollbackMetaInsert(mtr, record.Payload())
	case undo.DDLRecordTypeAllocateFileId:
		return true, r.rollbackAllocateFileId(record.Payload())
	default:
		return true, fmt.Errorf("access: unknown DDL record type: %s", record.RecordType())
	}
}

// rollbackCreateBTree は B+Tree 作成の取り消しを 1 単位進める
//   - 既にファイル丸ごと削除済みの場合は完了として true を返す
func (r *DDLRollbacker) rollbackCreateBTree(mtr *buffer.Mtr, payload []byte) (bool, error) {
	record, err := undo.DeserializeCreateBTreeUndoRecord(payload)
	if err != nil {
		return true, err
	}
	metaPageId := record.MetaPageId()
	if !r.bufferPool.HasHeapFile(metaPageId.FileId()) {
		return true, nil
	}
	tree := btree.NewTree(r.bufferPool, metaPageId)
	return tree.FreeStep(mtr)
}

// rollbackMetaInsert はカタログ Meta テーブルへの挿入を取り消す
//   - payload から MetaTableType + 主キーを復元し、 対応する Meta テーブルの Delete を呼ぶ
//   - 該当キーが既に存在しない (= 既に削除済み) の場合は何もしない
func (r *DDLRollbacker) rollbackMetaInsert(mtr *buffer.Mtr, payload []byte) error {
	record, err := undo.DeserializeMetaInsertUndoRecord(payload)
	if err != nil {
		return err
	}
	key := record.Key()
	var deleteErr error
	switch record.MetaTableType() {
	case undo.MetaTableTypeTable:
		deleteErr = r.catalog.TableMeta().Delete(mtr, key)
	case undo.MetaTableTypeIndex:
		deleteErr = r.catalog.IndexMeta().Delete(mtr, key)
	case undo.MetaTableTypeIndexKeyColumn:
		deleteErr = r.catalog.IndexKeyColumnMeta().Delete(mtr, key)
	case undo.MetaTableTypeColumn:
		deleteErr = r.catalog.ColumnMeta().Delete(mtr, key)
	case undo.MetaTableTypeConstraint:
		deleteErr = r.catalog.ConstraintMeta().Delete(mtr, key)
	default:
		return fmt.Errorf("access: unknown meta table type: %s", record.MetaTableType())
	}
	if errors.Is(deleteErr, btree.ErrKeyNotFound) {
		return nil
	}
	return deleteErr
}

// rollbackAllocateFileId は物理ファイル確保を取り消す
//   - payload から FileId を復元し、 該当の物理ファイルを削除する
//   - nextFileId は単調増加放置するため、 ここでは更新しない
//   - 2 回目の Rollback でも安全 (= 冪等)
func (r *DDLRollbacker) rollbackAllocateFileId(payload []byte) error {
	record, err := undo.DeserializeAllocateFileIdUndoRecord(payload)
	if err != nil {
		return err
	}
	return r.bufferPool.DeleteFile(record.FileId())
}
