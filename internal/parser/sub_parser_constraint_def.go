package parser

import (
	"errors"
	"minesql/internal/ast"
	"strings"
)

// constraintKind は制約の種類を表す直和型
type constraintKind int

const (
	constraintKindUnset     constraintKind = iota // 未決定
	constraintKindPK                              // PRIMARY KEY
	constraintKindUniqueKey                       // UNIQUE KEY
	constraintKindKey                             // KEY (非ユニーク)
)

type ConstraintDefParser struct {
	state  parserState                 // 現在のステート
	kind   constraintKind              // 制約の種類
	pkDef  ast.ConstraintPrimaryKeyDef // 生成される PK 定義
	ukDef  ast.ConstraintUniqueKeyDef  // 生成される UK 定義
	keyDef ast.ConstraintKeyDef        // 生成される KEY 定義 (非ユニーク)
	err    error                       // エラー情報
}

func NewConstraintDefParser() *ConstraintDefParser {
	return &ConstraintDefParser{
		state: CreateStateConstraint,
	}
}

func (cp *ConstraintDefParser) finalize() error {
	if cp.err != nil {
		return cp.err
	}

	// インデックス名の必須チェック (UNIQUE KEY / KEY)
	switch cp.kind {
	case constraintKindUniqueKey:
		if cp.ukDef.KeyName == "" {
			return errors.New("[parse error] index name is required for UNIQUE KEY")
		}
	case constraintKindKey:
		if cp.keyDef.KeyName == "" {
			return errors.New("[parse error] index name is required for KEY")
		}
	}

	// カラムが 1 つも指定されていない場合はエラー
	colCount := 0
	switch cp.kind {
	case constraintKindPK:
		colCount = len(cp.pkDef.Columns)
	case constraintKindUniqueKey:
		if cp.ukDef.Column.ColName != "" {
			colCount = 1
		}
	case constraintKindKey:
		if cp.keyDef.Column.ColName != "" {
			colCount = 1
		}
	}
	if colCount == 0 {
		return errors.New("[parse error] constraint definition requires at least one column")
	}

	return nil
}

func (cp *ConstraintDefParser) getDef() ast.Definition {
	switch cp.kind {
	case constraintKindPK:
		return &cp.pkDef
	case constraintKindUniqueKey:
		return &cp.ukDef
	case constraintKindKey:
		return &cp.keyDef
	default:
		return nil
	}
}

func (cp *ConstraintDefParser) onKeyword(word string) {
	if cp.err != nil {
		return
	}

	upper := strings.ToUpper(word)
	// 開始時の PRIMARY / UNIQUE / KEY 判定
	if cp.kind == constraintKindUnset {
		switch upper {
		case KPrimary:
			cp.kind = constraintKindPK
			cp.state = CreateStateConstraint
			return
		case KUnique:
			cp.kind = constraintKindUniqueKey
			cp.state = CreateStateConstraint
			return
		case KKey:
			// 単独の KEY: KEY index_name (column) の形式
			// KEY キーワード自体が KEY なので直接次の状態へ
			cp.kind = constraintKindKey
			cp.state = CreateStateConstraintKey
			return
		default:
			cp.setError(errors.New("[parse error] expected 'PRIMARY', 'UNIQUE', or 'KEY', got: " + word))
			return
		}
	}

	// KEY キーワードの処理 (PRIMARY KEY / UNIQUE KEY の 2 語目)
	if cp.state == CreateStateConstraint {
		if upper == KKey {
			cp.state = CreateStateConstraintKey
			return
		}
		cp.setError(errors.New("[parse error] expected 'KEY', got: " + word))
		return
	}

	cp.setError(errors.New("[parse error] unexpected keyword in constraint: " + word))
}

func (cp *ConstraintDefParser) onIdentifier(ident string) {
	if cp.err != nil {
		return
	}

	switch cp.state {
	case CreateStateConstraintKey:
		// KEY index_name / UNIQUE KEY index_name のパターン
		switch cp.kind {
		case constraintKindKey:
			if cp.keyDef.KeyName != "" {
				cp.setError(errors.New("[parse error] unexpected identifier (key name already set): " + ident))
				return
			}
			cp.keyDef.KeyName = ident
		case constraintKindUniqueKey:
			if cp.ukDef.KeyName != "" {
				cp.setError(errors.New("[parse error] unexpected identifier (key name already set): " + ident))
				return
			}
			cp.ukDef.KeyName = ident
		case constraintKindPK:
			// PRIMARY KEY には名前をつけないのでエラー
			cp.setError(errors.New("[parse error] unexpected identifier (PRIMARY KEY name not supported): " + ident))
		}
		return

	case CreateStateConstraintCol:
		// カラム名の追加
		colId := *ast.NewColumnId(ident)
		switch cp.kind {
		case constraintKindPK:
			cp.pkDef.Columns = append(cp.pkDef.Columns, colId)
		case constraintKindUniqueKey:
			cp.ukDef.Column = colId
		case constraintKindKey:
			cp.keyDef.Column = colId
		}
		cp.state = CreateStateConstraintWaitSeparator
		return
	}

	cp.setError(errors.New("[parse error] unexpected identifier: " + ident))
}

func (cp *ConstraintDefParser) onSymbol(symbol string) {
	if cp.err != nil {
		return
	}

	switch cp.state {
	case CreateStateConstraintKey:
		// "(" が来た場合はカラムリスト開始
		if symbol == string(SLeftParen) {
			cp.state = CreateStateConstraintCol
			return
		}
	case CreateStateConstraintWaitSeparator:
		// カラムリストの区切り文字処理
		// "," が来たら次のカラム待ち、")" が来たらカラムリスト終了
		if symbol == string(SComma) {
			cp.state = CreateStateConstraintCol
			return
		}
		if symbol == string(SRightParen) {
			return
		}
	}
	cp.setError(errors.New("[parse error] unexpected symbol in constraint: " + symbol))
}

func (cp *ConstraintDefParser) setError(err error) {
	if cp.err == nil {
		cp.err = err
	}
}
