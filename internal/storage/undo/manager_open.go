package undo

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// OpenManager は既存の Undo ファイルを開き、各 Undo ページを走査して entries と currentPageId を復元する
//   - 起動時のクラッシュリカバリ完了後に呼び出して、ディスク上の Undo レコードからメモリ状態を復元する用途
//   - 各 Undo ページの先頭から順にレコードを読み、trxId 単位で entries に積み直す
//   - NextPageNumber を辿った先頭ページから末尾ページまで走査し、末尾ページを currentPageId に設定する
func OpenManager(bp *buffer.Pool, fileId page.FileId) (*Manager, error) {
	m := &Manager{
		bufferPool: bp,
		fileId:     fileId,
		entries:    make(map[lock.TrxId][]Entry),
	}

	pageNum := page.PageNumber(1)
	for {
		pageId := page.NewId(fileId, pageNum)
		nextPageNum, err := m.restoreFromPage(pageId)
		if err != nil {
			return nil, err
		}
		m.currentPageId = pageId
		if nextPageNum == 0 {
			return m, nil
		}
		pageNum = nextPageNum
	}
}

// HistoryTrxIds は Undo ページ上に UPDATE / DELETE 種別の Undo を持つトランザクション ID を返す (= INSERT のみの trxId は除外)
func (m *Manager) HistoryTrxIds() []lock.TrxId {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]lock.TrxId, 0, len(m.entries))
	for trxId, entries := range m.entries {
		for _, e := range entries {
			if e.RecordType() == RecordTypeUpdate || e.RecordType() == RecordTypeDelete {
				result = append(result, trxId)
				break
			}
		}
	}
	return result
}

// restoreFromPage は 1 つの Undo ページを走査して entries に積み、次ページ番号を返す
func (m *Manager) restoreFromPage(pageId page.Id) (page.PageNumber, error) {
	bufPage, err := m.bufferPool.Page(pageId)
	if err != nil {
		return 0, err
	}
	defer m.bufferPool.Unpin(pageId)

	undoPage := NewPage(bufPage)
	offset := 0
	for offset < int(undoPage.UsedBytes()) {
		recordBytes := undoPage.Record(offset)
		if recordBytes == nil {
			break
		}
		fields, err := DeserializeFields(recordBytes)
		if err != nil {
			return 0, err
		}
		offset += len(recordBytes)
		record, err := fields.ToRecord()
		if err != nil {
			return 0, err
		}
		trxId := fields.TrxId()
		m.entries[trxId] = append(m.entries[trxId], NewEntry(trxId, record.RecordType(), record))
	}
	return undoPage.NextPageNumber(), nil
}
