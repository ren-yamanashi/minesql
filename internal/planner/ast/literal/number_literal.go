package literal

type NumberLiteral struct {
	LiteralType LiteralType
	Text        string
	Value       float64
}

func NewNumberLiteral(text string, value float64) *NumberLiteral {
	return &NumberLiteral{
		LiteralType: LiteralTypeNumber,
		Text:        text,
		Value:       value,
	}
}

func (nl *NumberLiteral) literalNode() {}
