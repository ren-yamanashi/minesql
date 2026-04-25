package server

import "minesql/internal/executor"

// resultType はクエリ結果の種別
type resultType int

const (
	resultOK        resultType = iota // DDL, DML, トランザクション制御
	resultResultSet                   // SELECT
)

// queryResult は executeQuery の構造化された結果
type queryResult struct {
	resultType   resultType
	affectedRows uint64
	columns      []columnDefPacket
	records      []executor.Record
}

// onComQuery は COM_QUERY を処理する
func (s *Server) onComQuery(cc *clientConn, sess *session, sql string) {
	result, err := s.executeQuery(sess, sql)
	if err != nil {
		_ = cc.writePacket((&errPacket{
			errorCode: erUnknownError,
			sqlState:  sqlStateGeneralError,
			message:   err.Error(),
		}).build())
		return
	}

	statusFlags := s.statusFlags(sess)
	switch result.resultType {
	case resultOK:
		_ = cc.writePacket((&okPacket{
			affectedRows: result.affectedRows,
			statusFlags:  statusFlags,
		}).build())
	case resultResultSet:
		_ = writeResultSet(cc, result, statusFlags)
	}
}

// writeResultSet は SELECT の結果セットを MySQL プロトコル形式で書き出す
func writeResultSet(cc *clientConn, result *queryResult, statusFlags uint16) error {
	colCount := len(result.columns)

	// 1. Column Count パケット
	if err := cc.writePacket(putLenEncInt(nil, uint64(colCount))); err != nil {
		return err
	}

	// 2. Column Definition パケット (カラム数分)
	for i := range result.columns {
		if err := cc.writePacket(result.columns[i].build()); err != nil {
			return err
		}
	}

	// 3. Row パケット (行数分)
	for _, record := range result.records {
		if err := cc.writePacket(buildRowPacket(record)); err != nil {
			return err
		}
	}

	// 4. OK_Packet (EOF 代替、CLIENT_DEPRECATE_EOF によりヘッダーは 0xFE)
	return cc.writePacket((&okPacket{statusFlags: statusFlags, isEOF: true}).build())
}

// buildRowPacket は Row パケットのペイロードを構築する
//
// 各フィールドを長さエンコード文字列で格納する
func buildRowPacket(record executor.Record) []byte {
	var buf []byte
	for _, field := range record {
		buf = putLenEncString(buf, string(field))
	}
	return buf
}
