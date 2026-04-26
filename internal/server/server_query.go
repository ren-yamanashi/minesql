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
	sql = strings.TrimSpace(sql)

	upperSQL := strings.ToUpper(sql)

	// SET 文は無視して OK を返す (go-sql-driver/mysql が接続時に SET NAMES utf8mb4 などを送るため)
	if strings.HasPrefix(upperSQL, "SET ") {
		return &queryResult{resultType: resultOK}, nil
	}

	// START TRANSACTION は BEGIN と同等として扱う
	if upperSQL == "START TRANSACTION" || strings.HasPrefix(upperSQL, "START TRANSACTION;") {
		sql = "BEGIN;"
	}

	// mysql クライアントは末尾のセミコロンを除去して送信するため、なければ補完する
	if !strings.HasSuffix(sql, ";") {
		sql += ";"
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

	// トランザクション外の DML は autocommit (一時的な trxId を発行して即 Commit)
	hdl := handler.Get()
	autocommit := sess.trxId == 0
	trxId := sess.trxId
	if autocommit {
		trxId = hdl.BeginTrx()
	}

	// 実行計画の作成
	plan, err := planner.Start(trxId, node)
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
		record, err := plan.Exec.Next()
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
	if plan.Columns != nil {
		columns := make([]columnDefPacket, len(plan.Columns))
		for i, col := range plan.Columns {
			columns[i] = columnDefPacket{tableName: col.TableName, name: col.ColName}
		}
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
