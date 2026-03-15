package evict

import "minesql/internal/storage/bufferpool/buftype"

// ClockSweep は Clock sweep アルゴリズムによりページ追い出しを行う
type ClockSweep struct {
	pointer    buftype.BufferId // 現在のポインタ位置 (次にチェックするバッファプールの index を指す) (例: pointer=0 の場合は BufferPages[0] をチェックする)
	maxSize    int              // バッファプールの最大サイズ
	referenced []bool           // 各バッファページの参照ビット
}

// NewClockSweep は size をバッファプールの最大サイズとして ClockSweep を初期化する
func NewClockSweep(size int) *ClockSweep {
	return &ClockSweep{
		pointer:    0,
		maxSize:    size,
		referenced: make([]bool, size),
	}
}

func (c *ClockSweep) Access(bufferId buftype.BufferId) {
	c.referenced[bufferId] = true
}

func (c *ClockSweep) Evict() buftype.BufferId {
	for {
		if c.referenced[c.pointer] {
			// 参照ビットをクリアし、次のページへ移動
			c.referenced[c.pointer] = false
			c.pointer = (c.pointer + 1) % buftype.BufferId(c.maxSize)
		} else {
			// 参照ビットがクリアされているページを追い出し対象とする
			victim := c.pointer
			c.pointer = (c.pointer + 1) % buftype.BufferId(c.maxSize)
			return victim
		}
	}
}

func (c *ClockSweep) Remove(bufferId buftype.BufferId) {
	c.referenced[bufferId] = false
}
