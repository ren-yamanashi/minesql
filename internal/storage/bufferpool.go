package storage

import (
	"fmt"
)

// BufferId は、バッファプール内のバッファページを識別するための ID (index)
type BufferId uint64

// PageTable は、PageId と BufferId の対応関係を管理するテーブル
//
// `PageId` に対応する BufferId を格納することで、該当のページがバッファプールのどの位置に格納されているかを特定できる
//
// - key: PageId (ページ ID)
//
// - value: BufferId (バッファ ID)
type PageTable map[PageId]BufferId

type BufferPool struct {
	diskManagers      map[FileId]*Disk // FileId → Disk のマップ
	bufferPages       []BufferPage     // バッファページのスライス
	maxBufferSize     int              // バッファプールの最大サイズ (バッファページ数)
	pageTable         PageTable        // ページテーブル (key: PageId, value: BufferId のマップ)
	evictionAlgorithm *LRU             // ページ追い出しアルゴリズム
}

// NewBufferPool は指定されたサイズの BufferPool を生成する
//
// size: バッファページの数 (例: 1000 を指定すると、1000 ページ分のバッファプールが生成される)
func NewBufferPool(size int) *BufferPool {
	return &BufferPool{
		diskManagers:      make(map[FileId]*Disk),
		bufferPages:       allocateBufferPages(size),
		maxBufferSize:     size,
		pageTable:         make(PageTable),
		evictionAlgorithm: NewLRU(size),
	}
}

// FetchPage は指定されたページ ID のバッファページをバッファプールから取得する
//
// ページがバッファプールに存在しない場合は、ディスクから読み込む
//
// 戻り値: 取得したバッファページ
func (bp *BufferPool) FetchPage(pageId PageId) (*BufferPage, error) {
	// ページテーブルにページがすでにある場合
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
	dm, err := bp.GetDisk(pageId.FileId)
	if err != nil {
		return nil, err
	}
	err = dm.ReadPageData(pageId, bufferPage.GetReadData())
	if err != nil {
		return nil, err
	}
	bufferPage.PageId = pageId
	bufferPage.IsDirty = false

	return bufferPage, nil
}

// AddPage はバッファプールに新しいページを追加する
//
// バッファプールに空きがある場合は新しいページを追加し、空きがない場合は古いページを新しいページに置き換える
//
// 戻り値: 追加されたページのバッファページ
func (bp *BufferPool) AddPage(pageId PageId) (*BufferPage, error) {
	// バッファに空きがある場合、新しいバッファページを追加し、ページテーブルを更新
	if len(bp.bufferPages) < bp.maxBufferSize {
		bp.bufferPages = append(bp.bufferPages, *NewBufferPage(pageId))
		bufferId := BufferId(len(bp.bufferPages) - 1)
		bp.addPageToTable(pageId, bufferId)
		bp.evictionAlgorithm.Access(bufferId)
		return &bp.bufferPages[bufferId], nil
	}

	// バッファに空きがない場合: 追い出しアルゴリズムでページを選択
	victimBufferId := bp.evictionAlgorithm.Evict()
	victim := &bp.bufferPages[victimBufferId]

	// 追い出すページがダーティーページであれば、ディスクに書き出す
	if victim.IsDirty {
		// FileId から Disk を取得
		dm, err := bp.GetDisk(victim.PageId.FileId)
		if err != nil {
			return nil, err
		}
		// ディスクに書き出す
		err = dm.WritePageData(victim.PageId, victim.Page)
		if err != nil {
			return nil, err
		}
	}

	// ページテーブルを更新 (追い出すページを削除し、新しいページを追加)
	bp.updatePageTable(victim.PageId, pageId, victimBufferId)

	// 新しいページに置き換え
	bp.bufferPages[victimBufferId] = *NewBufferPage(pageId)
	bp.evictionAlgorithm.Access(victimBufferId)
	return &bp.bufferPages[victimBufferId], nil
}

// FlushPage はバッファプール内のすべてのダーティーページをディスクに書き出す
func (bp *BufferPool) FlushPage() error {
	for pageId, bufferId := range bp.pageTable {
		bufferPage := &bp.bufferPages[bufferId]
		if !bufferPage.IsDirty {
			continue
		}

		// FileId から Disk を取得
		dm, err := bp.GetDisk(pageId.FileId)
		if err != nil {
			return err
		}

		// ダーティーページをディスクに書き出す
		err = dm.WritePageData(pageId, bufferPage.Page)
		if err != nil {
			return err
		}
		bufferPage.IsDirty = false
	}

	return nil
}

// UnRefPage は指定されたページの参照を解除し、優先的に追い出されるようにする
func (bp *BufferPool) UnRefPage(pageId PageId) {
	if bufferId, ok := bp.pageTable[pageId]; ok {
		bp.evictionAlgorithm.Remove(bufferId)
	}
}

// RegisterDisk は BufferPool に Disk を登録する
//
// fileId: 登録する Disk に対応する FileId
//
// dm: 登録する Disk
func (bp *BufferPool) RegisterDisk(fileId FileId, dm *Disk) {
	bp.diskManagers[fileId] = dm
}

// GetDisk は指定された FileId に対応する Disk を取得する
func (bp *BufferPool) GetDisk(fileId FileId) (*Disk, error) {
	dm, ok := bp.diskManagers[fileId]
	if !ok {
		return nil, fmt.Errorf("disk for FileId %d not found", fileId)
	}
	return dm, nil
}

// AllocatePageId は指定された FileId に対して新しい PageId を割り当てる
func (bp *BufferPool) AllocatePageId(fileId FileId) (PageId, error) {
	dm, err := bp.GetDisk(fileId)
	if err != nil {
		return INVALID_PAGE_ID, err
	}
	return dm.AllocatePage(), nil
}

// allocateBufferPages はバッファプール用のメモリ領域を確保する
func allocateBufferPages(size int) []BufferPage {
	// NOTE: 現状は Go のヒープ上にバッファページ用の領域を確保しているが、将来的には OS レベルの共有メモリなどを使用する方針に切り替える可能性がある
	pages := make([]BufferPage, size)
	for i := range pages {
		pages[i] = *NewBufferPage(INVALID_PAGE_ID) // 仮のページ ID で初期化 (実際にはバッファプールにページが追加されるときに設定される)
	}
	return pages
}

// updatePageTable はページテーブルを更新する
//
// evictPageId で指定したページが現在のバッファに属している場合のみ削除
//
// evictPageId: 追い出されるページの PageId
//
// newPageId: 追加されるページの PageId
//
// bufferId: 追い出されるページと追加されるページが属するバッファの BufferId
func (bp *BufferPool) updatePageTable(evictPageId PageId, newPageId PageId, bufferId BufferId) {
	if oldBufferId, ok := bp.pageTable[evictPageId]; ok && oldBufferId == bufferId {
		delete(bp.pageTable, evictPageId)
	}
	bp.pageTable[newPageId] = bufferId
}

// addPageToTable はページテーブルに新しいエントリを追加する
func (bp *BufferPool) addPageToTable(pageId PageId, bufferId BufferId) {
	bp.pageTable[pageId] = bufferId
}
