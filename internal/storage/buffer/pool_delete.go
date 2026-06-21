package buffer

import (
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// DeleteFile は指定 FileId に対応するヒープファイルを物理削除する
//   - fileId: 削除対象の FileId
//   - return: 該当 FileId が未登録の場合・HeapFile の Close に失敗した場合・物理ファイル削除が ENOENT 以外で失敗した場合にエラー
//   - Close 失敗時はキャッシュページが破棄され map エントリも物理ファイルも残った中途半端な状態となり、リトライ不可。呼び出し側は fatal として扱うこと
func (p *Pool) DeleteFile(fileId page.FileId) error {
	path, err := p.detachFile(fileId)
	if err != nil {
		return err
	}

	if err := os.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	return nil
}

// detachFile は該当 FileId のキャッシュページを破棄し、HeapFile を Close した上で map から取り除き、パスを返す
func (p *Pool) detachFile(fileId page.FileId) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	heapFile, ok := p.files[fileId]
	if !ok {
		return "", fmt.Errorf("heap file for FileId %d not found", fileId)
	}

	p.dropPagesForFile(fileId)

	if err := heapFile.Close(); err != nil {
		return "", err
	}
	delete(p.files, fileId)
	return heapFile.Path(), nil
}

// dropPagesForFile は該当 FileId のキャッシュページをページテーブル・フラッシュリスト・LRU から取り除く
func (p *Pool) dropPagesForFile(fileId page.FileId) {
	for i := range p.pages {
		bufPage := &p.pages[i]
		if bufPage.pageId.FileId() != fileId {
			continue
		}
		p.pageTable.delete(bufPage.pageId)
		p.flushList.delete(bufPage.pageId)
		bufPage.pageId = page.InvalidId()
		bufPage.isDirty = false
		bufPage.pinCount = 0
		bufPage.modifyCount = 0
		bufPage.oldestModificationLsn = 0
		p.lru.markUnused(id(i))
	}
}
