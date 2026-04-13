package access

import (
	"encoding/binary"
	"errors"
	"fmt"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/btree/node"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/encode"
	"minesql/internal/storage/lock"
	"minesql/internal/storage/log"
	"minesql/internal/storage/page"
)

// Table はテーブルへのアクセスを提供する
//
// 1 つの AccessMethod は 1 つの *.db (= 1 テーブル) ファイルに対応する
type Table struct {
	Name            string         // テーブル名
	MetaPageId      page.PageId    // テーブルの内容が入っている B+Tree のメタページの ID
	PrimaryKeyCount uint8          // プライマリキーの列数 (プライマリキーは先頭から連続している想定) (例: プライマリキーが (id, name) の場合、PrimaryKeyCount は 2 になる)
	UniqueIndexes   []*UniqueIndex // テーブルに紐づくユニークインデックス群
	undoLog         *UndoManager   // Undo ログ (nil の場合は Undo 記録をスキップ)
	redoLog         *log.RedoLog   // REDO ログ (nil の場合は REDO 記録をスキップ)
}

func NewTable(name string, metaPageId page.PageId, primaryKeyCount uint8, uniqueIndexes []*UniqueIndex, undoLog *UndoManager, redoLog *log.RedoLog) Table {
	return Table{
		Name:            name,
		MetaPageId:      metaPageId,
		PrimaryKeyCount: primaryKeyCount,
		UniqueIndexes:   uniqueIndexes,
		undoLog:         undoLog,
		redoLog:         redoLog,
	}
}

// Search は指定した検索モードでテーブルを検索し、TableIterator を返す
//
// ReadView に基づく Consistent Read を行う。ロックは取得しない。
func (t *Table) Search(bp *buffer.BufferPool, rv *ReadView, vr *VersionReader, mode RecordSearchMode) (*TableIterator, error) {
	btr := btree.NewBTree(t.MetaPageId)
	iterator, err := btr.Search(bp, mode.encode())
	if err != nil {
		return nil, err
	}
	return newTableIterator(iterator, bp, rv, vr), nil
}

// Create は空のテーブルを新規作成する
func (t *Table) Create(bp *buffer.BufferPool) error {
	// テーブルの B+Tree を作成
	tree, err := btree.CreateBTree(bp, t.MetaPageId)
	if err != nil {
		return err
	}
	t.MetaPageId = tree.MetaPageId

	// ユニークインデックスを作成
	for _, ui := range t.UniqueIndexes {
		err = ui.Create(bp)
		if err != nil {
			return err
		}
	}
	return nil
}

// Insert はテーブルに行を挿入する (Undo ログ記録 → 行の挿入 → 排他ロック取得の順で実行する)
//
// ソフトデリート済みの同一キーが存在する場合は Update で上書きする
func (t *Table) Insert(bp *buffer.BufferPool, trxId lock.TrxId, lockMgr *lock.Manager, columns [][]byte) error {
	// Undo ログを記録
	undoPtr := NullUndoPtr
	if t.undoLog != nil {
		ptr, err := t.undoLog.Append(trxId, UndoInsert, NewUndoInsertRecord(t, columns))
		if err != nil {
			return err
		}
		undoPtr = ptr
	}

	// 操作前の newlyDirtied をクリア (この操作でダーティーになったページだけを追跡するため)
	bp.ClearNewlyDirtied()

	// 行を挿入 → 排他ロックを取得
	if err := t.insert(bp, trxId, lockMgr, columns, undoPtr); err != nil {
		if t.undoLog != nil {
			t.undoLog.PopLast(trxId)
		}
		return err
	}

	// 新たにダーティーになったページの REDO ログを記録
	return t.appendRedoRecords(bp, trxId)
}

// SoftDelete はテーブルから行をソフトデリートする (Undo ログ記録 → 排他ロック取得 → ソフトデリートの順で実行する)
//
// B+Tree からレコードを物理削除せず、DeleteMark を 1 に設定する
func (t *Table) SoftDelete(bp *buffer.BufferPool, trxId lock.TrxId, lockMgr *lock.Manager, columns [][]byte) error {
	// Undo ログを記録 (既存行の lastModified/rollPtr を undo レコードに保存する)
	undoPtr := NullUndoPtr
	if t.undoLog != nil {
		prevLastModified, prevRollPtr, err := t.readCurrentVersion(bp, columns)
		if err != nil {
			return err
		}
		ptr, err := t.undoLog.Append(trxId, UndoDelete, NewUndoDeleteRecord(t, columns, prevLastModified, prevRollPtr))
		if err != nil {
			return err
		}
		undoPtr = ptr
	}

	// 操作前の newlyDirtied をクリア
	bp.ClearNewlyDirtied()

	// ロック取得 → ソフトデリート
	if err := t.softDelete(bp, trxId, lockMgr, columns, undoPtr); err != nil {
		if t.undoLog != nil {
			t.undoLog.PopLast(trxId)
		}
		return err
	}

	// 新たにダーティーになったページの REDO ログを記録
	return t.appendRedoRecords(bp, trxId)
}

// UpdateInplace はテーブルの行をインプレース更新する (Undo ログ記録 → 排他ロック取得 → 更新の順で実行する)
//
// プライマリキーが変わらないことを前提とする (プライマリキーが変わる場合は呼び出し側で SoftDelete + Insert を行う)
//
// ユニークインデックスは物理削除 (old) + 挿入 (new) で更新する
func (t *Table) UpdateInplace(bp *buffer.BufferPool, trxId lock.TrxId, lockMgr *lock.Manager, oldColumns [][]byte, newColumns [][]byte) error {
	// Undo ログを記録 (既存行の lastModified/rollPtr を undo レコードに保存する)
	undoPtr := NullUndoPtr
	if t.undoLog != nil {
		prevLastModified, prevRollPtr, err := t.readCurrentVersion(bp, oldColumns)
		if err != nil {
			return err
		}
		ptr, err := t.undoLog.Append(trxId, UndoUpdateInplace, NewUndoUpdateInplaceRecord(t, oldColumns, newColumns, prevLastModified, prevRollPtr))
		if err != nil {
			return err
		}
		undoPtr = ptr
	}

	// 操作前の newlyDirtied をクリア
	bp.ClearNewlyDirtied()

	// ロック取得 → 更新
	if err := t.updateInplace(bp, trxId, lockMgr, oldColumns, newColumns, undoPtr); err != nil {
		if t.undoLog != nil {
			t.undoLog.PopLast(trxId)
		}
		return err
	}

	// 新たにダーティーになったページの REDO ログを記録
	return t.appendRedoRecords(bp, trxId)
}

// GetUniqueIndexByName はインデックス名からユニークインデックスを取得する
func (t *Table) GetUniqueIndexByName(indexName string) (*UniqueIndex, error) {
	for _, ui := range t.UniqueIndexes {
		if ui.Name == indexName {
			return ui, nil
		}
	}
	return nil, fmt.Errorf("unique index %s not found in table %s", indexName, t.Name)
}

// LeafPageCount は B+Tree のメタページからリーフページ数を取得する
func (t *Table) LeafPageCount(bp *buffer.BufferPool) (uint64, error) {
	btr := btree.NewBTree(t.MetaPageId)
	return btr.LeafPageCount(bp)
}

// Height は B+Tree のメタページからツリーの高さを取得する
func (t *Table) Height(bp *buffer.BufferPool) (uint64, error) {
	btr := btree.NewBTree(t.MetaPageId)
	return btr.Height(bp)
}

// EncodeKey はカラム値からプライマリキー部分を Memcomparable format でエンコードする
func (t *Table) EncodeKey(columns [][]byte) []byte {
	var encoded []byte
	encode.Encode(columns[:t.PrimaryKeyCount], &encoded)
	return encoded
}

// encodeBTreeRecord はカラム値を B+Tree レコードに変換する
//
// Non-key 領域のレイアウト: [lastModified (8B)] [rollPtr (4B)] [非キーカラム (memcomparable)]
func (t *Table) encodeBTreeRecord(columns [][]byte, deleteMark byte, lastModified TrxId, rollPtr UndoPtr) node.Record {
	var key []byte
	encode.Encode(columns[:t.PrimaryKeyCount], &key)

	nonKey := encodeRecordNonKeyPrefix(lastModified, rollPtr)
	encode.Encode(columns[t.PrimaryKeyCount:], &nonKey)

	return node.NewRecord([]byte{deleteMark}, key, nonKey)
}

// insert は Undo 記録なしでテーブルに行を挿入する (行の挿入 → 排他ロック取得の順で実行する)
//
// 新規行は書き込み前に位置が確定しないため、ロック取得は挿入後に行う
func (t *Table) insert(bp *buffer.BufferPool, trxId lock.TrxId, lockMgr *lock.Manager, columns [][]byte, undoPtr UndoPtr) error {
	btr := btree.NewBTree(t.MetaPageId)

	btrRecord := t.encodeBTreeRecord(columns, 0, trxId, undoPtr)
	encodedKey := t.EncodeKey(columns)

	err := btr.Insert(bp, btrRecord)
	if err != nil {
		if !errors.Is(err, btree.ErrDuplicateKey) {
			return err
		}

		// 重複キーの場合、既存レコードがソフトデリート済みか確認する
		existing, _, findErr := btr.FindByKey(bp, encodedKey)
		if findErr != nil {
			return findErr
		}
		if existing.HeaderBytes()[0] != 1 {
			return btree.ErrDuplicateKey
		}

		// ソフトデリート済みなので Update で上書き (DeleteMark は 0 に戻る)
		err = btr.Update(bp, btrRecord)
		if err != nil {
			return err
		}
	}

	// 排他ロックを取得
	_, pos, err := btr.FindByKey(bp, encodedKey)
	if err != nil {
		return err
	}
	if err := lockMgr.Lock(trxId, pos, lock.Exclusive); err != nil {
		return err
	}

	// ユニークインデックスに挿入
	for _, ui := range t.UniqueIndexes {
		err := ui.Insert(bp, encodedKey, columns)
		if err != nil {
			return err
		}
	}

	return nil
}

// delete は Undo 記録なしでテーブルから行を物理削除する (排他ロック取得 → 物理削除の順で実行する)
func (t *Table) delete(bp *buffer.BufferPool, trxId lock.TrxId, lockMgr *lock.Manager, columns [][]byte) error {
	btr := btree.NewBTree(t.MetaPageId)

	// 対象行を検索
	encodedKey := t.EncodeKey(columns)
	_, pos, err := btr.FindByKey(bp, encodedKey)
	if err != nil {
		return err
	}

	// 排他ロックを取得
	if err := lockMgr.Lock(trxId, pos, lock.Exclusive); err != nil {
		return err
	}

	if err := btr.Delete(bp, encodedKey); err != nil {
		return err
	}

	// ユニークインデックスを物理削除
	for _, ui := range t.UniqueIndexes {
		err := ui.Delete(bp, encodedKey, columns)
		if err != nil {
			return err
		}
	}

	return nil
}

// softDelete は Undo 記録なしでテーブルから行をソフトデリートする (排他ロック取得 → ソフトデリートの順で実行する)
func (t *Table) softDelete(bp *buffer.BufferPool, trxId lock.TrxId, lockMgr *lock.Manager, columns [][]byte, undoPtr UndoPtr) error {
	btr := btree.NewBTree(t.MetaPageId)

	// 対象行を検索
	encodedKey := t.EncodeKey(columns)
	_, pos, err := btr.FindByKey(bp, encodedKey)
	if err != nil {
		return err
	}

	// 排他ロックを取得
	if err := lockMgr.Lock(trxId, pos, lock.Exclusive); err != nil {
		return err
	}

	btrRecord := t.encodeBTreeRecord(columns, 1, trxId, undoPtr)
	if err := btr.Update(bp, btrRecord); err != nil {
		return err
	}

	// ユニークインデックスをソフトデリート
	for _, ui := range t.UniqueIndexes {
		err := ui.Delete(bp, encodedKey, columns)
		if err != nil {
			return err
		}
	}

	return nil
}

// updateInplace は Undo 記録なしでテーブルの行をインプレース更新する (排他ロック取得 → 更新の順で実行する)
func (t *Table) updateInplace(bp *buffer.BufferPool, trxId lock.TrxId, lockMgr *lock.Manager, oldColumns [][]byte, newColumns [][]byte, undoPtr UndoPtr) error {
	btr := btree.NewBTree(t.MetaPageId)

	// 対象行を検索
	encodedKey := t.EncodeKey(oldColumns)
	_, pos, err := btr.FindByKey(bp, encodedKey)
	if err != nil {
		return err
	}

	// 排他ロックを取得
	if err := lockMgr.Lock(trxId, pos, lock.Exclusive); err != nil {
		return err
	}

	btrRecord := t.encodeBTreeRecord(newColumns, 0, trxId, undoPtr)
	if err := btr.Update(bp, btrRecord); err != nil {
		return err
	}

	// ユニークインデックスを更新 (物理削除 + Insert)
	encodedOldKey := t.EncodeKey(oldColumns)
	encodedNewKey := t.EncodeKey(newColumns)
	for _, ui := range t.UniqueIndexes {
		err := ui.Delete(bp, encodedOldKey, oldColumns)
		if err != nil {
			return err
		}
		err = ui.Insert(bp, encodedNewKey, newColumns)
		if err != nil {
			return err
		}
	}

	return nil
}

// readCurrentVersion は B+Tree 上の既存行から lastModified と rollPtr を読み取る
func (t *Table) readCurrentVersion(bp *buffer.BufferPool, columns [][]byte) (TrxId, UndoPtr, error) {
	btr := btree.NewBTree(t.MetaPageId)
	encodedKey := t.EncodeKey(columns)
	record, _, err := btr.FindByKey(bp, encodedKey)
	if err != nil {
		return 0, NullUndoPtr, err
	}
	lastModified, rollPtr, _ := decodeRecordNonKey(record.NonKeyBytes())
	return lastModified, rollPtr, nil
}

// appendRedoRecords は書き込みが行われたページの REDO レコードを追加する
func (t *Table) appendRedoRecords(bp *buffer.BufferPool, trxId TrxId) error {
	if t.redoLog == nil {
		return nil
	}
	for _, pid := range bp.PopNewlyDirtied() {
		data, err := bp.GetReadPageData(pid)
		if err != nil {
			return err
		}
		pg := page.NewPage(data)
		lsn := t.redoLog.AppendPageCopy(trxId, pid, data)
		binary.BigEndian.PutUint32(pg.Header, uint32(lsn))
	}
	return nil
}
