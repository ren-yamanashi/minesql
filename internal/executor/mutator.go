package executor

// Mutator はテーブルやインデックスに変更を加え、影響行数を返す
type Mutator interface {
	Execute() (int, error)
}
