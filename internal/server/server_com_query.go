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
	result, err := s.onQuery(sess, sql)
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
		deprecateEOF := sess.capability&clientDeprecateEOF != 0
		_ = writeResultSet(cc, result, statusFlags, deprecateEOF)
	}
}

// writeResultSet は SELECT の結果セットを MySQL プロトコル形式で書き出す
func writeResultSet(cc *clientConn, result *queryResult, statusFlags uint16, deprecateEOF bool) error {
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

	// 3. Column Definition の後の区切り
	if deprecateEOF {
		// CLIENT_DEPRECATE_EOF: 区切りなし (Row パケットに直接続く)
	} else {
		// EOF_Packet で Column Definition の終了を通知
		if err := cc.writePacket((&eofPacket{statusFlags: statusFlags}).build()); err != nil {
			return err
		}
	}

	// 4. Row パケット (行数分)
	for _, record := range result.records {
		if err := cc.writePacket(buildRowPacket(record)); err != nil {
			return err
		}
	}

	// 5. 結果セットの終了
	if deprecateEOF {
		// OK_Packet (ヘッダー 0xFE) で結果セットの終了を通知
		return cc.writePacket((&okPacket{statusFlags: statusFlags, isEOF: true}).build())
	}
	// EOF_Packet で結果セットの終了を通知
	return cc.writePacket((&eofPacket{statusFlags: statusFlags}).build())
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
