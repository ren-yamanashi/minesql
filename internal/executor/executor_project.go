package executor

// Project は InnerExecutor の結果から特定の列だけを返す
type Project struct {
	InnerExecutor Executor
	ColPos        []uint16 // 返す列の位置 (インデックス)
}

func NewProject(innerExecutor Executor, colPos []uint16) *Project {
	return &Project{
		InnerExecutor: innerExecutor,
		ColPos:        colPos,
	}
}

func (p *Project) Next() (Record, error) {
	// InnerExecutor からレコードを取得
	record, err := p.InnerExecutor.Next()
	if err != nil {
		return nil, err
	}

	// データがなくなったら終了
	if record == nil {
		return nil, nil
	}

	// 指定された列だけを返す
	projectedRecord := make(Record, len(p.ColPos))
	for i, pos := range p.ColPos {
		projectedRecord[i] = record[pos]
	}
	return projectedRecord, nil
}
