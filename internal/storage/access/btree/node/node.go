package node

const headerSize = 8

var (
	NODE_TYPE_INTERNAL = []byte("INTERNAL")
	NODE_TYPE_LEAF     = []byte("LEAF    ")
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

func (n *Node) InitAsInternalNode() {
	copy(n.data[0:8], NODE_TYPE_INTERNAL)
}

func (n *Node) InitAsLeafNode() {
	copy(n.data[0:8], NODE_TYPE_LEAF)
}
