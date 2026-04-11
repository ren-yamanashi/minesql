package log

// LSN はログシーケンス番号 (REDO ログのレコードを一意に識別する単調増加する番号)
type LSN uint32

// LSNGenerator は LSN を採番する
type LSNGenerator struct {
	LastGenerated LSN // 最後に採番した LSN (Next 未呼び出しの場合は初期値)
}

func NewLSNGenerator(initial LSN) *LSNGenerator {
	return &LSNGenerator{LastGenerated: initial}
}

// AllocateLSN は次の LSN を採番して返す
func (g *LSNGenerator) AllocateLSN() LSN {
	g.LastGenerated++
	return g.LastGenerated
}
