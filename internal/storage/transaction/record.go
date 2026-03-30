package transaction

import "minesql/internal/storage/buffer"

type LogRecord interface {
	Undo(bp *buffer.BufferPool) error
}

// TableOperator はトランザクションログの Undo に必要なテーブル操作を抽象化するインターフェース
//
// engine.TableHandler と互換性がある (engine パッケージの循環参照を避けるため別途定義)
type TableOperator interface {
	Insert(bp *buffer.BufferPool, columns [][]byte) error
	Delete(bp *buffer.BufferPool, columns [][]byte) error
	SoftDelete(bp *buffer.BufferPool, columns [][]byte) error
	UpdateInplace(bp *buffer.BufferPool, oldColumns, newColumns [][]byte) error
}
