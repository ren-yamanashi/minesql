package executor

type TrxManager struct {
	transactions map[TrxId]*Transaction
}

func NewTrxManager() *TrxManager {
	return &TrxManager{
		transactions: make(map[TrxId]*Transaction),
	}
}

func (m *TrxManager) AllocateTrxId() TrxId {
	var maxTrxId TrxId
	for trxId := range m.transactions {
		if trxId > maxTrxId {
			maxTrxId = trxId
		}
	}
	return maxTrxId + 1
}
