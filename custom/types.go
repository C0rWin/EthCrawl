package custom

import (
	"math/big"

	"github.com/ethereum/go-ethereum/core/types"
)

type Block struct {
	Number       *big.Int       `json:"number"`
	Transactions []*Transaction `json:"transactions"`
}

type Transaction struct {
	Hash     string   `json:"hash"`
	Nonce    uint64   `json:"nonce"`
	Value    string   `json:"value"`
	GasPrice *big.Int `json:"gas_price"`
	Gas      uint64   `json:"gas"`
	Type     uint8    `json:"type"`
	To       string   `json:"to"`
	Data     []byte   `json:"tx_data"`
}

func FromEthBlock(block *types.Block) (*Block, error) {
	b := &Block{
		Number:       block.Number(),
		Transactions: []*Transaction{},
	}

	for _, tx := range block.Transactions() {
		t := &Transaction{
			Hash:     tx.Hash().Hex(),
			Nonce:    tx.Nonce(),
			Value:    tx.Value().String(),
			GasPrice: tx.GasPrice(),
			Gas:      tx.Gas(),
			Type:     tx.Type(),
			Data:     tx.Data(),
		}
		// If not contract installement get To address
		if tx.To() != nil {
			t.To = tx.To().Hex()
		}
		b.Transactions = append(b.Transactions, t)
	}

	return b, nil
}
