package page

const PAGE_SIZE = 4096
const PageHeaderSize = 4 // 全ページ共通のヘッダーサイズ (Page LSN)

// Page は全ページ型共通のヘッダーとボディを持つ
type Page struct {
	Header []byte // data[0:4] - ページヘッダー
	Body   []byte // data[4:] - ページ型固有のデータ
}

// NewPage は raw data から Page を生成する
func NewPage(data []byte) *Page {
	return &Page{
		Header: data[:PageHeaderSize],
		Body:   data[PageHeaderSize:],
	}
}
