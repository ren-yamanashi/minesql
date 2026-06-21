package undo

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// ErrInvalidCreateBTreeUndoRecord は CreateBTreeUndoRecord のデシリアライズに失敗したことを表す
var ErrInvalidCreateBTreeUndoRecord = errors.New("undo: invalid create btree undo record")

// CreateBTreeUndoRecord は B+Tree 作成を取り消すための DDL Undo レコード
//   - metaPageId: 作成された B+Tree のメタページ ID。 Rollback 時はここから配下の全ページを辿って解放する
type CreateBTreeUndoRecord struct {
	metaPageId page.Id
}

func NewCreateBTreeUndoRecord(metaPageId page.Id) CreateBTreeUndoRecord {
	return CreateBTreeUndoRecord{metaPageId: metaPageId}
}

func (r CreateBTreeUndoRecord) MetaPageId() page.Id { return r.metaPageId }

// Serialize は CreateBTreeUndoRecord をバイト列にエンコードする
//   - return: MetaPageId のバイト列 (page.IdSize バイト)
func (r CreateBTreeUndoRecord) Serialize() []byte {
	return r.metaPageId.Bytes()
}

// DeserializeCreateBTreeUndoRecord はバイト列から CreateBTreeUndoRecord を復元する
func DeserializeCreateBTreeUndoRecord(data []byte) (CreateBTreeUndoRecord, error) {
	if len(data) != page.IdSize {
		return CreateBTreeUndoRecord{}, ErrInvalidCreateBTreeUndoRecord
	}
	return CreateBTreeUndoRecord{metaPageId: page.ReadId(data, 0)}, nil
}
