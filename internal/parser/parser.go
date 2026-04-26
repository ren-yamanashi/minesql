package parser

import (
	"errors"
	"strings"

	"github.com/ren-yamanashi/minesql/internal/ast"
)

type parserState int

const (
	StateInitial parserState = iota // 初期状態

	// -- SELECT Statement --

	SelectStateColumns   // SELECT 中
	SelectStateFrom      // FROM 中
	SelectStateInner     // INNER キーワード後、JOIN キーワード待ち
	SelectStateJoin      // JOIN キーワード後、テーブル名待ち
	SelectStateJoinTable // JOIN テーブル名取得後、ON キーワード待ち
	SelectStateOn        // ON 条件式の解析中
	SelectStateWhere     // WHERE 中
	SelectStateEnd       // SELECT Statement の終わり

	// -- INSERT Statement --

	InsertStateStart     // INSERT Statement の開始状態
	InsertStateInsert    // INSERT 中であり、INTO キーワード待ちの状態
	InsertStateInto      // INSERT INTO 中であり、テーブル名待ちの状態
	InsertStateTbName    // INSERT INTO <table_name> 中であり、カラムリスト開始 (`INSERT INTO <table_name>` の後の "(") 待ちの状態
	InsertStateColumns   // INSERT のカラムリスト中
	InsertStateEndCols   // INSERT のカラムリストが修了し、VALUES キーワード待ちの状態
	InsertStateValues    // INSERT の VALUES の値指定中 (`INSERT INTO ... VALUES ( ... )` の "(" 中) の状態
	InsertStateValueList // INSERT の値リスト中 (`INSERT INTO ... VALUES (val1, val2, ...)` の各値 (val1, val2, ...) の指定中の状態)
	InsertStateEnd       // INSERT Statement の終わり

	// -- CREATE Statement --

	CreateStateStart                   // CREATE Statement の開始状態
	CreateStateCreate                  // CREATE 中であり、TABLE キーワード待ちの状態
	CreateStateTable                   // CREATE TABLE 中であり、テーブル名待ちの状態
	CreateStateBodyStart               // CREATE TABLE の Body 部開始中 (CREATE TABLE <table_name> (...) の "(" 待ち) の状態
	CreateStateBody                    // CREATE TABLE の Body 部中
	CreateStateColDef                  // CREATE TABLE のカラム指定中であり、データ型定義待ちの状態
	CreateStateColWaitDefEnd           // CREATE TABLE のカラム定義修了待ち
	CreateStateConstraint              // CREATE TABLE の KEY 制約中
	CreateStateConstraintKey           // CREATE TABLE の PRIMARY KEY または UNIQUE KEY (現在は KEY キーワードの直後) の状態 | `UNIQUE KEY index_name` の "index_name" または `PRIMARY KEY (...)` の "(" 待ち
	CreateStateConstraintCol           // CREATE TABLE の KEY 制約のカラム名を指定中 (または指定待ち) の状態 | `PRIMARY KEY (col1, col2, ...)` または `UNIQUE KEY index_name (col1, col2, ...)` の "(" の直後か、"," の直後の状態
	CreateStateConstraintWaitSeparator // CREATE TABLE の KEY 制約のカラムリスト区切り文字 ("," または ")") 待ち
	CreateStateConstraintFKName        // CREATE TABLE の FOREIGN KEY 制約中であり、FK 名待ちの状態 | `FOREIGN KEY fk_name (...)` の "fk_name" 待ち
	CreateStateConstraintFKCol         // CREATE TABLE の FOREIGN KEY 制約中であり、カラムリスト開始待ちの状態 | `FOREIGN KEY fk_name (...)` の "(" 待ち
	CreateStateConstraintFKColName     // CREATE TABLE の FOREIGN KEY 制約のカラム名を指定中の状態 | `FOREIGN KEY fk_name (col)` の "col" 待ち
	CreateStateConstraintFKColEnd      // CREATE TABLE の FOREIGN KEY 制約のカラムリスト終了待ちの状態 | `FOREIGN KEY fk_name (col)` の ")" 待ち
	CreateStateConstraintFKReferences  // CREATE TABLE の FOREIGN KEY 制約中であり、REFERENCES キーワード待ちの状態
	CreateStateConstraintFKRefTable    // CREATE TABLE の FOREIGN KEY 制約中であり、参照先テーブル名待ちの状態 | `REFERENCES ref_table (...)` の "ref_table" 待ち
	CreateStateConstraintFKRefColOpen  // CREATE TABLE の FOREIGN KEY 制約中であり、参照先カラムリスト開始待ちの状態 | `REFERENCES ref_table (...)` の "(" 待ち
	CreateStateConstraintFKRefColName  // CREATE TABLE の FOREIGN KEY 制約の参照先カラム名を指定中の状態 | `REFERENCES ref_table (ref_col)` の "ref_col" 待ち
	CreateStateConstraintFKRefColEnd   // CREATE TABLE の FOREIGN KEY 制約の参照先カラムリスト終了待ちの状態 | `REFERENCES ref_table (ref_col)` の ")" 待ち
	CreateStateEnd                     // CREATE Statement の終わり

	// -- DELETE Statement --

	DeleteStateDelete // DELETE 中であり、FROM キーワード待ちの状態
	DeleteStateFrom   // DELETE FROM 中であり、テーブル名待ちの状態
	DeleteStateWhere  // DELETE の WHERE 中
	DeleteStateEnd    // DELETE Statement の終わり

	// -- UPDATE Statement --

	UpdateStateUpdate // UPDATE 中であり、テーブル名待ちの状態
	UpdateStateTable  // UPDATE <table_name> 中であり、SET キーワード待ちの状態
	UpdateStateSet    // SET 句のカラム名待ちの状態
	UpdateStateSetCol // SET 句のカラム名の後、"=" 待ちの状態
	UpdateStateSetEq  // SET 句の "=" の後、値待ちの状態
	UpdateStateSetVal // SET 句の値の後、"," or WHERE or ";" 待ちの状態
	UpdateStateWhere  // UPDATE の WHERE 中
	UpdateStateEnd    // UPDATE Statement の終わり

	// -- ALTER USER Statement --

	// -- START TRANSACTION Statement --

	StartTxStateStart // START キーワード後、TRANSACTION キーワード待ち
	StartTxStateEnd   // START TRANSACTION の終わり

	// -- ALTER USER Statement --

	AlterUserStateAlter      // ALTER キーワード後、USER キーワード待ち
	AlterUserStateUser       // USER キーワード後、ユーザー名 (文字列リテラル) 待ち
	AlterUserStateUsername   // ユーザー名取得後、"@" 待ち
	AlterUserStateAt         // "@" 取得後、ホスト名 (文字列リテラル) 待ち
	AlterUserStateHost       // ホスト名取得後、IDENTIFIED キーワード待ち
	AlterUserStateIdentified // IDENTIFIED キーワード後、BY キー��ード待ち
	AlterUserStateBy         // BY キーワード後、パスワード (文字列リテラル) 待ち
	AlterUserStateEnd        // ALTER USER Statement の終わり
)

type Parser struct {
	currentParser StatementParser // 現在のステートに対応するハンドラ
}

func NewParser() *Parser {
	return &Parser{}
}

// Parse は SQL 文を解析し AST を構築する
func (p *Parser) Parse(sql string) (ast.Statement, error) {
	p.currentParser = nil
	tokenizer := NewTokenizer(sql, p)
	tokenizer.Tokenize()

	if p.currentParser == nil {
		return nil, errors.New("[parse error] no statement parsed")
	}

	p.currentParser.finalize()

	if err := p.currentParser.getError(); err != nil {
		return nil, err
	}

	return p.currentParser.getResult(), nil
}

func (p *Parser) onKeyword(word string) {
	if p.currentParser != nil {
		p.currentParser.onKeyword(word)
		return
	}

	upper := strings.ToUpper(word)

	// 最初のキーワードに応じて、適切な StatementParser を生成してデリゲートする
	switch upper {
	case KSelect:
		p.currentParser = NewSelectParser()
		p.currentParser.onKeyword(word)
		return

	case KCreate:
		p.currentParser = NewCreateParser()
		p.currentParser.onKeyword(word)
		return

	case KInsert:
		p.currentParser = NewInsertParser()
		p.currentParser.onKeyword(word)
		return

	case KDelete:
		p.currentParser = NewDeleteParser()
		p.currentParser.onKeyword(word)
		return

	case KUpdate:
		p.currentParser = NewUpdateParser()
		p.currentParser.onKeyword(word)
		return

	case KAlter:
		p.currentParser = NewAlterUserParser()
		p.currentParser.onKeyword(word)
		return

	case KStart:
		p.currentParser = NewStartTransactionParser()
		p.currentParser.onKeyword(word)
		return

	// トランザクション系はキーワードのみで構成されるため OnKeyword のデリゲートは不要
	case KBegin:
		p.currentParser = NewTransactionParser(ast.TxBegin)
		return

	case KCommit:
		p.currentParser = NewTransactionParser(ast.TxCommit)
		return

	case KRollback:
		p.currentParser = NewTransactionParser(ast.TxRollback)
		return
	}
}

func (p *Parser) onIdentifier(ident string) {
	if p.currentParser != nil {
		p.currentParser.onIdentifier(ident)
		return
	}
}

func (p *Parser) onSymbol(symbol string) {
	if p.currentParser != nil {
		p.currentParser.onSymbol(symbol)
		return
	}
}

func (p *Parser) onString(value string) {
	if p.currentParser != nil {
		p.currentParser.onString(value)
		return
	}
}

func (p *Parser) onNumber(num string) {
	if p.currentParser != nil {
		p.currentParser.onNumber(num)
		return
	}
}

func (p *Parser) onComment(text string) {
	if p.currentParser != nil {
		p.currentParser.onComment(text)
		return
	}
}

func (p *Parser) onError(err error) {
	if p.currentParser != nil {
		p.currentParser.onError(err)
		return
	}
}
