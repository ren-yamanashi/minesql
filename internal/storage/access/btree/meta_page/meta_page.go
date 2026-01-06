package metapage

import (
	"encoding/binary"
	"minesql/internal/storage/disk"
)

type MetaPage struct {
	// メタページ全体のバイト列
	data []byte
}

func NewMetaPage(data []byte) *MetaPage {
	return &MetaPage{data: data}
}

// メタページのヘッダーからルートページ ID を読み取る
func (mp *MetaPage) RootPageId() disk.PageId {
	return disk.PageId(binary.LittleEndian.Uint64(mp.data[0:8]))
}

// メタページのヘッダーにルートページ ID を設定する
func (mp *MetaPage) SetRootPageId(rootPageId disk.PageId) {
	binary.LittleEndian.PutUint64(mp.data[0:8], uint64(rootPageId))
}
