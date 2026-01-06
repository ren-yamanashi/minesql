package node

const headerSize = 8

var (
	NODE_TYPE_LEAF   = []byte("LEAF    ")
	NODE_TYPE_BRANCH = []byte("BRANCH  ")
)

type NodeHeader struct {
	NodeType [8]byte
}

type Node struct {
	// ページデータ全体
	data []byte
}

func NewNode(data []byte) *Node {
	return &Node{data: data}
}

func (n *Node) NodeType() []byte {
	return n.data[0:8]
}

func (n *Node) Body() []byte {
	return n.data[headerSize:]
}

func (n *Node) InitAsBranchNode() {
	copy(n.data[0:8], NODE_TYPE_BRANCH)
}

func (n *Node) InitAsLeafNode() {
	copy(n.data[0:8], NODE_TYPE_LEAF)
}
