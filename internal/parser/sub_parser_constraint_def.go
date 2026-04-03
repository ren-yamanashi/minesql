package parser

import (
	"errors"
	"minesql/internal/ast"
	"strings"
)

type ConstraintDefParser struct {
	state parserState                  // 現在のステート
	isPK  bool                         // PK か UK かのフラグ
	pkDef *ast.ConstraintPrimaryKeyDef // 生成される PK 定義
	ukDef *ast.ConstraintUniqueKeyDef  // 生成される UK 定義
	err   error                        // エラー情報
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

	// カラムが1つも指定されていない場合はエラー
	var colCount = func() int {
		if cp.isPK {
			return len(cp.pkDef.Columns)
		}
		if cp.ukDef.Column.ColName != "" {
			return 1
		}
		return 0
	}()
	if colCount == 0 {
		return errors.New("[parse error] constraint definition requires at least one column")
	}

	return nil
}

func (cp *ConstraintDefParser) getDef() ast.Definition {
	if cp.isPK {
		return cp.pkDef
	}
	return cp.ukDef
}

func (cp *ConstraintDefParser) onKeyword(word string) {
	if cp.err != nil {
		return
	}

	upper := strings.ToUpper(word)
	// 開始時の PRIMARY / UNIQUE 判定
	if cp.pkDef == nil && cp.ukDef == nil {
		switch upper {
		case KPrimary:
			cp.isPK = true
			cp.pkDef = &ast.ConstraintPrimaryKeyDef{}
			cp.state = CreateStateConstraint
			return
		case KUnique:
			cp.isPK = false
			cp.ukDef = &ast.ConstraintUniqueKeyDef{}
			cp.state = CreateStateConstraint
			return
		default:
			cp.setError(errors.New("[parse error] expected 'PRIMARY' or 'UNIQUE', got: " + word))
			return
		}
	}

	// KEY キーワードの処理
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
		// UNIQUE KEY index_name ( ... ) のパターン
		if !cp.isPK {
			if cp.ukDef.KeyName != "" {
				cp.setError(errors.New("[parse error] unexpected identifier (key name already set): " + ident))
				return
			}
			cp.ukDef.KeyName = ident
			return
		}
		// PRIMARY KEY には通常名前をつけないのでエラー
		cp.setError(errors.New("[parse error] unexpected identifier (PRIMARY KEY name not supported): " + ident))
		return

	case CreateStateConstraintCol:
		// カラム名の追加
		// PK の場合は PK 定義のカラムリストに追加、UK の場合は UK 定義のカラムにセット
		colId := *ast.NewColumnId(ident)
		if cp.isPK {
			cp.pkDef.Columns = append(cp.pkDef.Columns, colId)
		} else {
			cp.ukDef.Column = colId
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
