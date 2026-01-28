package literal

type StringLiteral struct {
	LiteralType LiteralType
	Text        string
	Value       string
}

func NewStringLiteral(text string, value string) *StringLiteral {
	return &StringLiteral{
		LiteralType: LiteralTypeString,
		Text:        text,
		Value:       value,
	}
}

func (sl *StringLiteral) ToBytes() []byte {
	return []byte(sl.Value)
}

func (sl *StringLiteral) ToString() string {
	return sl.Value
}

func (sl *StringLiteral) literalNode() {}
