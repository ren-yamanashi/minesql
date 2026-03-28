package undo

type UndoLogRecord interface {
	Undo() error
}
