package parser

import (
	"github.com/forbole/juno/v4/types"
)

// SumGasTxs returns the total gas consumed by a set of transactions.
func SumGasTxs(txs []*types.Tx) uint64 {
	var totalGas uint64

	for _, tx := range txs {
		totalGas += uint64(tx.GasUsed)
	}

	return totalGas
}
