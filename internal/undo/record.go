package undo

type LogRecord interface {
	Undo() error
}
