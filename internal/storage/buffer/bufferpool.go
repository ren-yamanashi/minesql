package buffer

import (
	"encoding/binary"
	"fmt"
	"minesql/internal/storage/file"
	"minesql/internal/storage/log"
	"minesql/internal/storage/page"
	"sync"
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
	mutex             sync.RWMutex
	disks             map[page.FileId]*file.Disk // FileId → Disk のマップ
	bufferPages       []BufferPage               // バッファページのスライス
	maxBufferSize     int                        // バッファプールの最大サイズ (バッファページ数)
	pageTable         PageTable                  // ページテーブル (key: PageId, value: BufferId のマップ)
	evictionAlgorithm *LRU                       // ページ追い出しアルゴリズム
	redoLog           *log.RedoLog               // REDO ログ
	flushList         *FlushList                 // ダーティーページのフラッシュリスト
	newlyDirtied      []page.PageId              // 前回の PopNewlyDirtied 以降にダーティーになったページ
}

// NewBufferPool は指定されたサイズの BufferPool を生成する
//   - size: バッファページの最大数 (例: 1000 を指定すると、最大 1000 ページを格納できるバッファプールが生成される)
//   - redoLog: REDO ログ
func NewBufferPool(size int, redoLog *log.RedoLog) *BufferPool {
	return &BufferPool{
		disks:             make(map[page.FileId]*file.Disk),
		bufferPages:       make([]BufferPage, 0, size),
		maxBufferSize:     size,
		pageTable:         make(PageTable),
		evictionAlgorithm: NewLRU(size),
		redoLog:           redoLog,
		flushList:         NewFlushList(),
	}
}

// GetWritePageData は書き込み用にページデータを取得する
func (bp *BufferPool) GetWritePageData(pageId page.PageId) ([]byte, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	bufPage, err := bp.fetchPage(pageId)
	if err != nil {
		return nil, err
	}

	// ページが clean ならダーティーページにし、フラッシュリストに追加する
	if !bufPage.IsDirty {
		bufPage.IsDirty = true
		bp.flushList.Add(pageId)
	}
	bp.newlyDirtied = append(bp.newlyDirtied, pageId)
	return bufPage.Page, nil
}

// GetReadPageData は読み込み用にページデータを取得する
func (bp *BufferPool) GetReadPageData(pageId page.PageId) ([]byte, error) {
	// ページがプール内にある場合は RLock で返す (LRU 更新不要)
	bp.mutex.RLock()
	if bufferId, ok := bp.pageTable[pageId]; ok {
		bufPage := &bp.bufferPages[bufferId]
		bp.mutex.RUnlock()
		return bufPage.Page, nil
	}
	bp.mutex.RUnlock()

	// ページがプールにない場合はディスクから読み込む
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	bufPage, err := bp.fetchPage(pageId)
	if err != nil {
		return nil, err
	}
	return bufPage.Page, nil
}

// FetchPage は指定されたページ ID のバッファページをバッファプールから取得する
//
// ページがバッファプールに存在しない場合は、ディスクから読み込む
func (bp *BufferPool) FetchPage(pageId page.PageId) (*BufferPage, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	return bp.fetchPage(pageId)
}

// AddPage はバッファプールに新しいページを追加する
//
// バッファプールに空きがある場合は新しいページを追加し、空きがない場合は古いページを新しいページに置き換える
func (bp *BufferPool) AddPage(pageId page.PageId) error {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	_, err := bp.addPage(pageId)
	return err
}

// MaxBufferSize はバッファプールの最大サイズ (バッファページ数) を返す
func (bp *BufferPool) MaxBufferSize() int {
	return bp.maxBufferSize // 不変値のためロック不要
}

// UnRefPage は指定されたページの参照を解除し、優先的に追い出されるようにする
func (bp *BufferPool) UnRefPage(pageId page.PageId) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	if bufferId, ok := bp.pageTable[pageId]; ok {
		bp.evictionAlgorithm.Remove(bufferId)
	}
}

// RegisterDisk は BufferPool に Disk を登録する
//   - fileId: 登録する Disk に対応する FileId
//   - disk: 登録する Disk
func (bp *BufferPool) RegisterDisk(fileId page.FileId, disk *file.Disk) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	bp.disks[fileId] = disk
}

// GetDisk は指定された FileId に対応する Disk を取得する
func (bp *BufferPool) GetDisk(fileId page.FileId) (*file.Disk, error) {
	bp.mutex.RLock()
	defer bp.mutex.RUnlock()
	return bp.getDisk(fileId)
}

// AllocatePageId は指定された FileId に対して新しい PageId を割り当てる
func (bp *BufferPool) AllocatePageId(fileId page.FileId) (page.PageId, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	disk, err := bp.getDisk(fileId)
	if err != nil {
		return page.InvalidPageId, err
	}
	return disk.AllocatePage(), nil
}

// getDisk は指定された FileId に対応する Disk を取得する (mutex 取得済みの状態で呼び出す必要がある)
func (bp *BufferPool) getDisk(fileId page.FileId) (*file.Disk, error) {
	disk, ok := bp.disks[fileId]
	if !ok {
		return nil, fmt.Errorf("disk for FileId %d not found", fileId)
	}
	return disk, nil
}

// fetchPage は指定されたページをバッファプールから取得する (mutex 取得済みの状態で呼び出す必要がある)
func (bp *BufferPool) fetchPage(pageId page.PageId) (*BufferPage, error) {
	// ページがバッファプールにある場合
	if bufferId, ok := bp.pageTable[pageId]; ok {
		bufferPage := &bp.bufferPages[bufferId]
		bp.evictionAlgorithm.Access(bufferId)
		return bufferPage, nil
	}

	// ページがバッファプールにない場合
	bufferPage, err := bp.addPage(pageId)
	if err != nil {
		return nil, err
	}

	// ディスクからページを読み込む
	disk, err := bp.getDisk(pageId.FileId)
	if err != nil {
		return nil, err
	}
	err = disk.ReadPageData(pageId, bufferPage.Page)
	if err != nil {
		return nil, err
	}
	bufferPage.PageId = pageId
	bufferPage.IsDirty = false

	return bufferPage, nil
}

// addPage はバッファプールに新しいページを追加する (mutex 取得済みの状態で呼び出す必要がある)
func (bp *BufferPool) addPage(pageId page.PageId) (*BufferPage, error) {
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

	// 追い出すページがダーティーページであればディスクに書き出す
	if victim.IsDirty {
		// victim の Page LSN 以上の REDO ログがフラッシュされていることを確認
		if bp.redoLog != nil {
			pg := page.NewPage(victim.Page)
			pageLSN := log.LSN(binary.BigEndian.Uint32(pg.Header))
			if pageLSN > bp.redoLog.FlushedLSN() {
				// フラッシュされていない場合は REDO ログをフラッシュ
				if err := bp.redoLog.Flush(); err != nil {
					return nil, err
				}
			}
		}

		// FileId から Disk を取得
		disk, err := bp.getDisk(victim.PageId.FileId)
		if err != nil {
			return nil, err
		}

		// ディスクに書き出す
		err = disk.WritePageData(victim.PageId, victim.Page)
		if err != nil {
			return nil, err
		}

		// フラッシュリストから除外
		bp.flushList.Remove(victim.PageId)
	}

	// ページテーブルを更新 (追い出すページを削除し、新しいページを追加)
	bp.updatePageTable(victim.PageId, pageId, victimBufferId)

	// 新しいページに置き換え
	bp.bufferPages[victimBufferId] = *NewBufferPage(pageId)
	bp.evictionAlgorithm.Access(victimBufferId)
	return &bp.bufferPages[victimBufferId], nil
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
