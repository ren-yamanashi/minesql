package access

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

type newSecondaryIndexInput struct {
	MetaPageId  page.Id            // セカンダリインデックスの MetaPageId
	PrimaryTree *btree.Tree        // プライマリインデックスの B+Tree
	IndexId     dictionary.IndexId // インデックス ID
	IndexName   string             // インデックス名
	Unique      bool               // ユニークインデックスか
	Lock        *lock.Manager
	UndoLog     *undo.Manager
}

type secondaryIndex struct {
	catalog     *dictionary.Catalog
	bufferPool  *buffer.Pool
	tree        *btree.Tree        // セカンダリインデックスの B+Tree
	primaryTree *btree.Tree        // プライマリインデックスの B+Tree
	fileId      page.FileId        // インデックスが属するテーブルの FileId
	indexId     dictionary.IndexId // インデックス ID
	indexName   string             // インデックス名
	unique      bool               // ユニーク制約の有無
	lock        *lock.Manager
	undoLog     *undo.Manager
}

// newSecondaryIndex は既存のセカンダリインデックスを開く
func newSecondaryIndex(
	ct *dictionary.Catalog,
	bp *buffer.Pool,
	input newSecondaryIndexInput,
) *secondaryIndex {
	tree := btree.NewTree(bp, input.MetaPageId)
	return &secondaryIndex{
		catalog:     ct,
		bufferPool:  bp,
		tree:        tree,
		primaryTree: input.PrimaryTree,
		fileId:      input.PrimaryTree.MetaPageId().FileId(),
		indexId:     input.IndexId,
		indexName:   input.IndexName,
		unique:      input.Unique,
		lock:        input.Lock,
		undoLog:     input.UndoLog,
	}
}

type createSecondaryIndexInput struct {
	FileId      page.FileId        // インデックスが属するテーブルの FileId
	PrimaryTree *btree.Tree        // プライマリインデックスの B+Tree
	IndexId     dictionary.IndexId // インデックス ID
	IndexName   string             // インデックス名
	Unique      bool               // ユニークか
	Lock        *lock.Manager
	UndoLog     *undo.Manager
}

// createSecondaryIndex は空のセカンダリインデックスを作成する
//   - mtr: B+Tree 作成と後続の DDL Undo Append / Meta Insert を同一スコープで記録する Mtr。 Commit は呼び出し側
//   - 対応する CreateBTreeUndo は呼び出し側 (= createSecondaryIndexes) が同じ mtr に Append する
func createSecondaryIndex(
	mtr *buffer.Mtr,
	ct *dictionary.Catalog,
	bp *buffer.Pool,
	input createSecondaryIndexInput,
) (*secondaryIndex, error) {
	tree, err := btree.CreateTree(bp, input.FileId, mtr)
	if err != nil {
		return nil, err
	}
	return &secondaryIndex{
		catalog:     ct,
		bufferPool:  bp,
		tree:        tree,
		primaryTree: input.PrimaryTree,
		fileId:      input.PrimaryTree.MetaPageId().FileId(),
		indexId:     input.IndexId,
		indexName:   input.IndexName,
		unique:      input.Unique,
		lock:        input.Lock,
		undoLog:     input.UndoLog,
	}, nil
}

// search は指定した検索モードでインデックスを検索し、イテレータを返す
//   - readView が非 nil の場合は MVCC の可視性判定 + Undo 遡及を行う
func (si *secondaryIndex) search(mtr *buffer.Mtr, mode SearchMode, readView *readView) (*SecondaryIndexIterator, error) {
	iter, err := si.tree.Search(mtr, mode.Encode())
	if err != nil {
		return nil, err
	}
	return NewSecondaryIndexIterator(si.indexName, iter, si.catalog, si.bufferPool, si.primaryTree, readView, si.undoLog), nil
}

// insert は行を挿入する
//   - unique index の場合かつセカンダリキーの重複があるとエラー
//   - 論理削除済みの同一キー (SK + PK) が存在する場合は上書きする
func (si *secondaryIndex) insert(mtr *buffer.Mtr, record *SecondaryRecord, trxId lock.TrxId) error {
	if si.unique {
		// ユニーク制約の重複確認の直前に SK 単位の排他ロックを取り、
		// 同じ SK で異なる PK を持つ並行 INSERT / 未コミット論理削除との競合を直列化する
		uniqueRowKey := lock.RowKey{MetaPageId: si.tree.MetaPageId(), Key: record.encodedSecondaryKey()}
		if err := si.lock.Lock(trxId, uniqueRowKey, lock.Exclusive); err != nil {
			return err
		}
		if err := si.checkUnique(mtr, record); err != nil {
			return err
		}
	}
	encodedRecord := record.Encode()

	// 排他ロックを取得
	rowKey := lock.RowKey{MetaPageId: si.tree.MetaPageId(), Key: encodedRecord.Key()}
	if err := si.lock.Lock(trxId, rowKey, lock.Exclusive); err != nil {
		return err
	}

	// 挿入
	err := si.tree.Insert(mtr, encodedRecord)
	// 重複キーエラーの場合、既存のレコードが論理削除済みか確認
	if errors.Is(err, btree.ErrDuplicateKey) {
		existing, _, findErr := si.tree.FindByKey(mtr, encodedRecord.Key())
		if findErr != nil {
			return findErr
		}
		deleteMark := existing.Header()[0]
		// 論理削除済みでない場合はエラー
		if deleteMark != 1 {
			return btree.ErrDuplicateKey
		}
		// 論理削除済みの場合は上書き
		return si.tree.Update(mtr, encodedRecord)
	}
	return err
}

// delete は行を物理削除する
func (si *secondaryIndex) delete(mtr *buffer.Mtr, record *SecondaryRecord, trxId lock.TrxId) error {
	encodedRecord := record.Encode()

	// 排他ロックを取得
	rowKey := lock.RowKey{MetaPageId: si.tree.MetaPageId(), Key: encodedRecord.Key()}
	if err := si.lock.Lock(trxId, rowKey, lock.Exclusive); err != nil {
		return err
	}

	// 物理削除
	return si.tree.Delete(mtr, encodedRecord.Key())
}

// softDelete は行を論理削除する
func (si *secondaryIndex) softDelete(mtr *buffer.Mtr, record *SecondaryRecord, trxId lock.TrxId) error {
	encodedRecord := record.Encode()

	if si.unique {
		// 並行 INSERT との直列化のため、SK 単位の排他ロックも併用する
		uniqueRowKey := lock.RowKey{MetaPageId: si.tree.MetaPageId(), Key: record.encodedSecondaryKey()}
		if err := si.lock.Lock(trxId, uniqueRowKey, lock.Exclusive); err != nil {
			return err
		}
	}

	// 排他ロックを取得
	rowKey := lock.RowKey{MetaPageId: si.tree.MetaPageId(), Key: encodedRecord.Key()}
	if err := si.lock.Lock(trxId, rowKey, lock.Exclusive); err != nil {
		return err
	}

	// 論理削除
	// deleteMark を 1 にしたレコードで上書き
	deleted, err := NewSecondaryRecord(si.catalog, si.bufferPool, NewSecondaryRecordInput{
		fileId:     si.fileId,
		deleteMark: 1,
		lastTrxId:  trxId,
		indexName:  si.indexName,
		colNames:   record.colNames,
		values:     record.values,
		pk:         record.pk,
	})
	if err != nil {
		return err
	}
	return si.tree.Update(mtr, deleted.Encode())
}

// leafPageCount はリーフページ数を取得する
//   - mtr: メタページの S-latch / Pin を保持する mini-transaction。 解放は呼び出し側
func (si *secondaryIndex) leafPageCount(mtr *buffer.Mtr) (uint64, error) {
	return si.tree.LeafPageCount(mtr)
}

// height はツリーの高さを取得する
//   - mtr: メタページの S-latch / Pin を保持する mini-transaction。 解放は呼び出し側
func (si *secondaryIndex) height(mtr *buffer.Mtr) (uint64, error) {
	return si.tree.Height(mtr)
}

// checkUnique は record のセカンダリキーに対して active なレコードが存在するか確認する
//   - return: 存在する場合は ErrDuplicateKey
func (si *secondaryIndex) checkUnique(mtr *buffer.Mtr, sr *SecondaryRecord) error {
	encodedSk := sr.encodedSecondaryKey()
	// セカンダリインデックスのキーは SK+PK の構成であり、SK のみで SearchModeKey を使うと SK 以上の最初のキーの位置に着地する
	iter, err := si.tree.Search(mtr, btree.SearchModeKey{Key: encodedSk})
	if err != nil {
		return err
	}

	for {
		existing, ok, err := iter.Get()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}

		// キー同士の比較
		if len(existing.Key()) < len(encodedSk) {
			return nil
		}
		existingSk := existing.Key()[:len(encodedSk)]
		if !bytes.Equal(existingSk, encodedSk) {
			return nil
		}

		deleteMark := existing.Header()[0]
		// 論理削除済みでない場合は重複
		if deleteMark != 1 {
			return btree.ErrDuplicateKey
		}
		// 論理削除済みの場合は次のレコードへ
		if err := iter.Advance(); err != nil {
			return err
		}
	}
}
