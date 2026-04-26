package server

// packet は単一のパケットとして直列化できる型を表す
type packet interface {
	build() []byte
}
