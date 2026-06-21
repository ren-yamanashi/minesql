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
//   - catalog: Meta テーブル削除と freeListMap ページ ID 取得のために共有する
type DDLRollbacker struct {
	bufferPool *buffer.Pool
	catalog    *dictionary.Catalog
}

func NewDDLRollbacker(bp *buffer.Pool, catalog *dictionary.Catalog) *DDLRollbacker {
	return &DDLRollbacker{bufferPool: bp, catalog: catalog}
}

// Rollback は DDL Undo レコード 1 件を取り消す
//   - record.RecordType() で分岐し、 種別ごとの取消処理を呼ぶ
//   - 全ての書き込みは mtr 経由で行われ Redo に記録される
func (r *DDLRollbacker) Rollback(mtr *buffer.Mtr, record undo.DDLRecord) error {
	switch record.RecordType() {
	case undo.DDLRecordTypeCreateBTree:
		return r.rollbackCreateBTree(mtr, record.Payload())
	case undo.DDLRecordTypeMetaInsert:
		return r.rollbackMetaInsert(mtr, record.Payload())
	case undo.DDLRecordTypeAllocateFileId:
		return r.rollbackAllocateFileId(mtr, record.Payload())
	default:
		return fmt.Errorf("access: unknown DDL record type: %s", record.RecordType())
	}
}

// rollbackCreateBTree は B+Tree 作成を取り消す
//   - payload から MetaPageId を復元し、 配下の全ページを子→親 (= 末尾要素から先頭要素) の順に Deallocate する
//   - 既にファイル丸ごと削除済み、 もしくはメタページがフリーリストに含まれている (= 既に解放済み) の場合は何もしない
func (r *DDLRollbacker) rollbackCreateBTree(mtr *buffer.Mtr, payload []byte) error {
	record, err := undo.DeserializeCreateBTreeUndoRecord(payload)
	if err != nil {
		return err
	}
	metaPageId := record.MetaPageId()
	if !r.bufferPool.HasHeapFile(metaPageId.FileId()) {
		return nil
	}
	freeListMapPageId := r.catalog.FreeListMapPageId()
	if _, err := mtr.PageForWrite(freeListMapPageId); err != nil {
		return err
	}
	defer mtr.Unpin(freeListMapPageId)

	inFreeList, err := r.bufferPool.IsPageInFileFreeList(mtr, freeListMapPageId, metaPageId)
	if err != nil {
		return err
	}
	if inFreeList {
		return nil
	}
	tree := btree.NewTree(r.bufferPool, metaPageId)
	pageIds, err := tree.AllPageIds(mtr)
	if err != nil {
		return err
	}
	for i := len(pageIds) - 1; i >= 0; i-- {
		if err := r.bufferPool.Deallocate(mtr, freeListMapPageId, pageIds[i]); err != nil {
			return err
		}
	}
	return nil
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
func (r *DDLRollbacker) rollbackAllocateFileId(_ *buffer.Mtr, payload []byte) error {
	record, err := undo.DeserializeAllocateFileIdUndoRecord(payload)
	if err != nil {
		return err
	}
	return r.bufferPool.DeleteFile(record.FileId())
}
