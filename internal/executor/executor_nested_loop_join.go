package executor

// NestedLoopJoin は Nested Loop Join を実行する
//
// 外側 (左) の各行に対して、buildRightExec で内側 (右) の Executor を生成し、左右のレコードを結合して返す
type NestedLoopJoin struct {
	leftExec       Executor
	buildRightExec func(leftRecord Record) (Executor, error)

	currentLeft  Record
	currentRight Executor
}

func NewNestedLoopJoin(
	leftExec Executor,
	buildRightExec func(leftRecord Record) (Executor, error),
) *NestedLoopJoin {
	return &NestedLoopJoin{
		leftExec:       leftExec,
		buildRightExec: buildRightExec,
	}
}

func (nlj *NestedLoopJoin) Next() (Record, error) {
	for {
		// 現在の右 Executor からレコード取得を試みる (初回は nil なのでスキップ)
		if nlj.currentRight != nil {
			rightRecord, err := nlj.currentRight.Next()
			if err != nil {
				return nil, err
			}
			if rightRecord != nil {
				return nlj.concatenate(nlj.currentLeft, rightRecord), nil
			}
		}

		// 左 Executor から次の行を取得
		leftRecord, err := nlj.leftExec.Next()
		if err != nil {
			return nil, err
		}
		if leftRecord == nil {
			return nil, nil
		}

		// 新しい右 Executor を生成
		nlj.currentLeft = leftRecord
		nlj.currentRight, err = nlj.buildRightExec(leftRecord)
		if err != nil {
			return nil, err
		}
	}
}

// concatenate は左右のレコードを結合する
func (nlj *NestedLoopJoin) concatenate(left, right Record) Record {
	result := make(Record, len(left)+len(right))
	copy(result, left)
	copy(result[len(left):], right)
	return result
}
