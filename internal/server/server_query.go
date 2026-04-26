package server

import (
	"fmt"
	"strings"

	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/parser"
	"minesql/internal/planner"
	"minesql/internal/storage/acl"
	"minesql/internal/storage/handler"
)

// handleQuery は SQL をパースし、ステートメントの種類に応じて適切なハンドラにディスパッチする
func (s *Server) onQuery(sess *session, sql string) (*queryResult, error) {
	sql = strings.TrimSpace(sql)

	// SET 文は無視して OK を返す (go-sql-driver/mysql が接続時に SET NAMES utf8mb4 などを送るため)
	if strings.HasPrefix(strings.ToUpper(sql), "SET ") {
		return &queryResult{resultType: resultOK}, nil
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
		return s.executeTransaction(sess, txStmt)
	}

	// それ以外は planner を通して実行する
	result, err := s.executeQuery(sess, node)
	if err != nil {
		return nil, err
	}

	// ALTER USER の場合はオンメモリの ACL を再構築する
	if alterStmt, ok := node.(*ast.AlterUserStmt); ok {
		hdl := handler.Get()
		user, _ := hdl.Catalog.GetUserByName(alterStmt.Username)
		s.acl = acl.NewACLFromCatalog(user.Username, user.Host, user.AuthString)
	}

	return result, nil
}

// executeTransaction はトランザクション制御文 (BEGIN/COMMIT/ROLLBACK) を実行する
func (s *Server) executeTransaction(sess *session, stmt *ast.TransactionStmt) (*queryResult, error) {
	switch stmt.Kind {
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
	default:
		return nil, fmt.Errorf("unknown transaction kind: %d", stmt.Kind)
	}
}

// executeQuery は planner で実行計画を作成し、executor で実行する
//
// トランザクション外の場合は autocommit で実行する
func (s *Server) executeQuery(sess *session, node ast.Statement) (*queryResult, error) {
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
