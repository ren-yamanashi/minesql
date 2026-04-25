package server

import (
	"fmt"
	"strings"

	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/parser"
	"minesql/internal/planner"
	"minesql/internal/storage/handler"
)

// executeQuery はクエリを実行して構造化された結果を返す
func (s *Server) executeQuery(sess *session, sql string) (*queryResult, error) {
	// SET 文は無視して OK を返す (go-sql-driver/mysql が接続時に SET NAMES utf8mb4 などを送るため)
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "SET ") {
		return &queryResult{resultType: resultOK}, nil
	}

	p := parser.NewParser()
	node, err := p.Parse(sql)
	if err != nil {
		return nil, err
	}

	// トランザクション制御は planner を通さず直接処理する
	if txStmt, ok := node.(*ast.TransactionStmt); ok {
		switch txStmt.Kind {
		case ast.TxBegin:
			if sess.trxId != 0 {
				return nil, fmt.Errorf("transaction already started")
			}
			sess.trxId = handler.Get().BeginTrx()
			return &queryResult{resultType: resultOK}, nil
		case ast.TxCommit:
			if sess.trxId == 0 {
				return nil, fmt.Errorf("no active transaction")
			}
			if err := handler.Get().CommitTrx(sess.trxId); err != nil {
				return nil, err
			}
			sess.trxId = 0
			return &queryResult{resultType: resultOK}, nil
		case ast.TxRollback:
			if sess.trxId == 0 {
				return nil, fmt.Errorf("no active transaction")
			}
			if err := handler.Get().RollbackTrx(sess.trxId); err != nil {
				return nil, err
			}
			sess.trxId = 0
			return &queryResult{resultType: resultOK}, nil
		}
	}

	// SELECT の場合、カラム情報を取得する
	var columns []columnDefPacket
	if selectStmt, ok := node.(*ast.SelectStmt); ok {
		columns = resolveColumnInfo(selectStmt)
	}

	// トランザクション外の DML は autocommit (一時的な trxId を発行して即 Commit)
	hdl := handler.Get()
	autocommit := sess.trxId == 0
	trxId := sess.trxId
	if autocommit {
		trxId = hdl.BeginTrx()
	}

	// 実行計画の作成
	exec, err := planner.Start(trxId, node)
	if err != nil {
		if autocommit {
			_ = hdl.RollbackTrx(trxId)
		}
		return nil, err
	}

	// クエリの実行
	var records []executor.Record
	var affectedRows uint64
	for {
		record, err := exec.Next()
		if err != nil {
			if autocommit {
				_ = hdl.RollbackTrx(trxId)
			}
			return nil, err
		}
		if record == nil {
			break
		}
		records = append(records, record)
		affectedRows++
	}

	if autocommit {
		if err := hdl.CommitTrx(trxId); err != nil {
			return nil, err
		}
	}

	// SELECT の場合は結果セット、それ以外は OK
	if columns != nil {
		return &queryResult{
			resultType: resultResultSet,
			columns:    columns,
			records:    records,
		}, nil
	}

	return &queryResult{
		resultType:   resultOK,
		affectedRows: affectedRows,
	}, nil
}

// resolveColumnInfo は SELECT 文のカラム情報を解決する
func resolveColumnInfo(stmt *ast.SelectStmt) []columnDefPacket {
	hdl := handler.Get()
	tableName := stmt.From.TableName

	// JOIN の場合
	if len(stmt.Joins) > 0 {
		var columns []columnDefPacket
		tables := []string{tableName}
		for _, join := range stmt.Joins {
			tables = append(tables, join.Table.TableName)
		}

		if stmt.Columns == nil {
			// SELECT *: 全テーブルの全カラムを順序位置でソートして結合
			for _, tbl := range tables {
				tblMeta, ok := hdl.Catalog.GetTableMetaByName(tbl)
				if !ok {
					continue
				}
				for _, col := range tblMeta.GetSortedCols() {
					columns = append(columns, columnDefPacket{tableName: tbl, name: col.Name})
				}
			}
		} else {
			for _, col := range stmt.Columns {
				columns = append(columns, columnDefPacket{tableName: col.TableName, name: col.ColName})
			}
		}
		return columns
	}

	// 単一テーブルの場合
	if stmt.Columns == nil {
		// SELECT *: テーブルの全カラムを順序位置でソートして取得
		tblMeta, ok := hdl.Catalog.GetTableMetaByName(tableName)
		if !ok {
			return nil
		}
		columns := make([]columnDefPacket, 0, len(tblMeta.GetSortedCols()))
		for _, col := range tblMeta.GetSortedCols() {
			columns = append(columns, columnDefPacket{tableName: tableName, name: col.Name})
		}
		return columns
	}

	// カラム指定の場合
	columns := make([]columnDefPacket, 0, len(stmt.Columns))
	for _, col := range stmt.Columns {
		tbl := col.TableName
		if tbl == "" {
			tbl = tableName
		}
		columns = append(columns, columnDefPacket{tableName: tbl, name: col.ColName})
	}
	return columns
}
