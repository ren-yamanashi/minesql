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

func (sl *StringLiteral) literalNode() {}
