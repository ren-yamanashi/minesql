package redo

// Lsn はログシーケンス番号 (Redo ログレコードを一意に識別するための番号)
// 1 からの連番
type Lsn uint32
