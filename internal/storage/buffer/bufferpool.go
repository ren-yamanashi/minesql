package buffer

import (
	"encoding/binary"
	"fmt"
	"minesql/internal/storage/file"
	"minesql/internal/storage/log"
	"minesql/internal/storage/page"
)

// BufferId は、バッファプール内のバッファページを識別するための ID (index)
type BufferId uint64

// PageTable は PageId と BufferId の対応関係を管理するテーブル
//
// PageId に対応する BufferId を格納することで、該当のページがバッファプールのどの位置に格納されているかを特定できる
//
//   - key: PageId (ページ ID)
//   - value: BufferId (バッファ ID)
type PageTable map[page.PageId]BufferId

type BufferPool struct {
	disks             map[page.FileId]*file.Disk // FileId → Disk のマップ
	bufferPages       []BufferPage               // バッファページのスライス
	maxBufferSize     int                        // バッファプールの最大サイズ (バッファページ数)
	pageTable         PageTable                  // ページテーブル (key: PageId, value: BufferId のマップ)
	evictionAlgorithm *LRU                       // ページ追い出しアルゴリズム
	redoLog           *log.RedoLog               // REDO ログ
	FlushList         *FlushList                 // ダーティーページのフラッシュリスト
}

// NewBufferPool は指定されたサイズの BufferPool を生成する
//   - size: バッファページの数 (例: 1000 を指定すると、1000 ページ分のバッファプールが生成される)
//   - redoLog: REDO ログ
func NewBufferPool(size int, redoLog *log.RedoLog) *BufferPool {
	bufPages := make([]BufferPage, size)
	for i := range bufPages {
		bufPages[i] = *NewBufferPage(page.INVALID_PAGE_ID) // 仮のページ ID で初期化 (実際にはバッファプールにページが追加されるときに設定される)
	}
	return &BufferPool{
		disks:             make(map[page.FileId]*file.Disk),
		bufferPages:       bufPages,
		maxBufferSize:     size,
		pageTable:         make(PageTable),
		evictionAlgorithm: NewLRU(size),
		redoLog:           redoLog,
		FlushList:         NewFlushList(),
	}
}

// FetchPage は指定されたページ ID のバッファページをバッファプールから取得する
//
// ページがバッファプールに存在しない場合は、ディスクから読み込む
func (bp *BufferPool) FetchPage(pageId page.PageId) (*BufferPage, error) {
	// ページがバッファプールにある場合
	if bufferId, ok := bp.pageTable[pageId]; ok {
		bufferPage := &bp.bufferPages[bufferId]
		bp.evictionAlgorithm.Access(bufferId)
		return bufferPage, nil
	}

	// ページがバッファプールにない場合
	bufferPage, err := bp.AddPage(pageId)
	if err != nil {
		return nil, err
	}

	// ディスクからページを読み込む
	disk, err := bp.GetDisk(pageId.FileId)
	if err != nil {
		return nil, err
	}
	err = disk.ReadPageData(pageId, bufferPage.GetReadData())
	if err != nil {
		return nil, err
	}
	bufferPage.PageId = pageId
	bufferPage.IsDirty = false

	return bufferPage, nil
}

// AddPage はバッファプールに新しいページを追加する (追加されたページのバッファページを返す)
//
// バッファプールに空きがある場合は新しいページを追加し、空きがない場合は古いページを新しいページに置き換える
func (bp *BufferPool) AddPage(pageId page.PageId) (*BufferPage, error) {
	// バッファに空きがある場合、新しいバッファページを追加し、ページテーブルを更新 (エントリを追加)
	if len(bp.bufferPages) < bp.maxBufferSize {
		bp.bufferPages = append(bp.bufferPages, *NewBufferPage(pageId))
		bufferId := BufferId(len(bp.bufferPages) - 1)
		bp.pageTable[pageId] = bufferId
		bp.evictionAlgorithm.Access(bufferId)
		return &bp.bufferPages[bufferId], nil
	}

	// バッファに空きがない場合: 追い出しアルゴリズムでページを選択
	victimBufferId := bp.evictionAlgorithm.Evict()
	victim := &bp.bufferPages[victimBufferId]

	// 追い出すページがダーティーページであれば、ディスクに書き出す
	if victim.IsDirty {
		// victim の Page LSN 以上の REDO ログがフラッシュされていることを確認
		if bp.redoLog != nil {
			pg := page.NewPage(victim.Page)
			pageLSN := log.LSN(binary.BigEndian.Uint32(pg.Header))
			if pageLSN > bp.redoLog.FlushedLSN {
				// フラッシュされていない場合は、REDO ログをフラッシュ
				if err := bp.redoLog.Flush(); err != nil {
					return nil, err
				}
			}
		}

		// FileId から Disk を取得
		disk, err := bp.GetDisk(victim.PageId.FileId)
		if err != nil {
			return nil, err
		}
		// ディスクに書き出す
		err = disk.WritePageData(victim.PageId, victim.Page)
		if err != nil {
			return nil, err
		}

		// フラッシュリストから除外
		bp.FlushList.Remove(victim.PageId)
	}

	// ページテーブルを更新 (追い出すページを削除し、新しいページを追加)
	bp.updatePageTable(victim.PageId, pageId, victimBufferId)

	// 新しいページに置き換え
	bp.bufferPages[victimBufferId] = *NewBufferPage(pageId)
	bp.evictionAlgorithm.Access(victimBufferId)
	return &bp.bufferPages[victimBufferId], nil
}

// FlushPage はバッファプール内のすべてのダーティーページをディスクに書き出す
func (bp *BufferPool) FlushAllPages() error {
	// REDO ログを先にフラッシュ
	if bp.redoLog != nil {
		if err := bp.redoLog.Flush(); err != nil {
			return err
		}
	}

	// 全ダーティーページをディスクに書き出す
	for pageId, bufferId := range bp.pageTable {
		bufferPage := &bp.bufferPages[bufferId]
		if !bufferPage.IsDirty {
			continue
		}

		// FileId から Disk を取得
		disk, err := bp.GetDisk(pageId.FileId)
		if err != nil {
			return err
		}

		// ダーティーページをディスクに書き出す
		err = disk.WritePageData(pageId, bufferPage.Page)
		if err != nil {
			return err
		}
		bufferPage.IsDirty = false
	}

	// フラッシュリストをクリア
	bp.FlushList.Clear()

	// 全ディスクを Sync してストレージデバイスへの書き込みを保証
	for _, disk := range bp.disks {
		if err := disk.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// FlushOldestPages はフラッシュリストの先頭から n ページをディスクにフラッシュする
func (bp *BufferPool) FlushOldestPages(n int) error {
	// フラッシュリストの先頭 (最も古いダーティーページ) から n 件取得
	pageIds := bp.FlushList.OldestPageIds(n)
	if len(pageIds) == 0 {
		return nil
	}

	// ダーティーページをディスクに書き出す前に、REDO ログバッファを先にフラッシュする
	if bp.redoLog != nil {
		if err := bp.redoLog.Flush(); err != nil {
			return err
		}
	}

	// フラッシュ対象のディスクを記録する (後でまとめて Sync するため)
	syncDisks := make(map[page.FileId]bool)

	for _, pid := range pageIds {
		// ページテーブルからバッファページを取得
		bufferId, ok := bp.pageTable[pid]
		if !ok {
			continue
		}

		// 既にクリーンなページはフラッシュリストから除外するだけ
		bufferPage := &bp.bufferPages[bufferId]
		if !bufferPage.IsDirty {
			bp.FlushList.Remove(pid)
			continue
		}

		// ダーティーページをディスクに書き出す
		disk, err := bp.GetDisk(pid.FileId)
		if err != nil {
			return err
		}
		if err := disk.WritePageData(pid, bufferPage.Page); err != nil {
			return err
		}

		// ページをクリーンにし、フラッシュリストから除外
		bufferPage.IsDirty = false
		bp.FlushList.Remove(pid)
		syncDisks[pid.FileId] = true
	}

	// フラッシュしたページのディスクを Sync してストレージデバイスへの書き込みを保証
	for fileId := range syncDisks {
		disk, err := bp.GetDisk(fileId)
		if err != nil {
			return err
		}
		if err := disk.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// UnRefPage は指定されたページの参照を解除し、優先的に追い出されるようにする
func (bp *BufferPool) UnRefPage(pageId page.PageId) {
	if bufferId, ok := bp.pageTable[pageId]; ok {
		bp.evictionAlgorithm.Remove(bufferId)
	}
}

// RegisterDisk は BufferPool に Disk を登録する
//   - fileId: 登録する Disk に対応する FileId
//   - disk: 登録する Disk
func (bp *BufferPool) RegisterDisk(fileId page.FileId, disk *file.Disk) {
	bp.disks[fileId] = disk
}

// GetDisk は指定された FileId に対応する Disk を取得する
func (bp *BufferPool) GetDisk(fileId page.FileId) (*file.Disk, error) {
	disk, ok := bp.disks[fileId]
	if !ok {
		return nil, fmt.Errorf("disk for FileId %d not found", fileId)
	}
	return disk, nil
}

// AllocatePageId は指定された FileId に対して新しい PageId を割り当てる
func (bp *BufferPool) AllocatePageId(fileId page.FileId) (page.PageId, error) {
	disk, err := bp.GetDisk(fileId)
	if err != nil {
		return page.INVALID_PAGE_ID, err
	}
	return disk.AllocatePage(), nil
}

// DirtyPageIds はダーティーページの PageId リストを返す
func (bp *BufferPool) DirtyPageIds() []page.PageId {
	var pages []page.PageId
	for pageId, bufferId := range bp.pageTable {
		if bp.bufferPages[bufferId].IsDirty {
			pages = append(pages, pageId)
		}
	}
	return pages
}

// updatePageTable はページテーブルを更新する
//
// evictPageId で指定したページが現在のバッファに属している場合のみ削除
//
//   - evictPageId: 追い出されるページの PageId
//   - newPageId: 追加されるページの PageId
//   - bufferId: 追い出されるページと追加されるページが属するバッファの BufferId
func (bp *BufferPool) updatePageTable(evictPageId page.PageId, newPageId page.PageId, bufferId BufferId) {
	if oldBufferId, ok := bp.pageTable[evictPageId]; ok && oldBufferId == bufferId {
		delete(bp.pageTable, evictPageId)
	}
	bp.pageTable[newPageId] = bufferId
}
