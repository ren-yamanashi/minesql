package statement

import "minesql/internal/planner/ast/definition"

type KeywordType string

const (
	KeywordTable KeywordType = "table"
)

type CreateTableStmt struct {
	StmtType          StmtType
	Keyword           KeywordType
	TableName         string
	CreateDefinitions []definition.Definition
}

func NewCreateTableStmt(tableName string, createDefinitions []definition.Definition) *CreateTableStmt {
	return &CreateTableStmt{
		StmtType:          StmtTypeCreate,
		Keyword:           KeywordTable,
		TableName:         tableName,
		CreateDefinitions: createDefinitions,
	}
}

func (stmt *CreateTableStmt) statementNode() {}
