package node

const nodeHeaderSize = 8

var (
	NODE_TYPE_LEAF   = []byte("LEAF    ")
	NODE_TYPE_BRANCH = []byte("BRANCH  ")
)

type NodeHeader struct {
	NodeType [8]byte
}

// Node は B+Tree のノードを表す interface
type Node interface {
	// ノードタイプヘッダーを除いたボディ部分を取得する
	Body() []byte
}

// ページデータからノードタイプを取得する
// 戻り値: NODE_TYPE_LEAF or NODE_TYPE_BRANCH
func GetNodeType(data []byte) []byte {
	return data[0:8]
}
