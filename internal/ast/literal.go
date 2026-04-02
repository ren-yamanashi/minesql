package ast

type Literal interface {
	ToString() string
	ToBytes() []byte
}

// ===========================
// String
// ===========================

type StringLiteral struct {
	Value string
}

func NewStringLiteral(value string) *StringLiteral {
	return &StringLiteral{
		Value: value,
	}
}

func (sl *StringLiteral) ToBytes() []byte {
	return []byte(sl.Value)
}

func (sl *StringLiteral) ToString() string {
	return sl.Value
}
