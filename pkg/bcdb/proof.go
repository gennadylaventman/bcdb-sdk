// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"bytes"
	"encoding/json"

	"github.com/IBM-Blockchain/bcdb-server/pkg/crypto"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

type TxProof struct {
	intermediateHashes [][]byte
}

func (p *TxProof) Verify(receipt *types.TxReceipt, tx proto.Message) (bool, error) {
	txEnv, ok := tx.(*types.DataTxEnvelope)
	if !ok {
		return false, errors.Errorf("tx [%s] is not data transaction, only data transaction supported so far", tx.String())
	}
	valInfo := receipt.GetHeader().GetValidationInfo()[receipt.GetTxIndex()]
	txBytes, err := json.Marshal(txEnv)
	if err != nil {
		return false, errors.Wrapf(err, "can't serialize tx [%s] to json", tx.String())
	}
	viBytes, err := json.Marshal(valInfo)
	if err != nil {
		return false, errors.Wrapf(err, "can't serialize validation info [%s] to json", valInfo.String())
	}
	txHash, err := crypto.ComputeSHA256Hash(append(txBytes, viBytes...))
	if err != nil {
		return false, errors.Wrap(err, "can't calculate concatenated hash of tx and its validation info")
	}
	var currHash []byte
	for i, pHash := range p.intermediateHashes {
		if i == 0 {
			if !bytes.Equal(txHash, pHash) {
				return false, nil
			}
			currHash = txHash
			continue
		}
		currHash, err = crypto.ConcatenateHashes(currHash, pHash)
		if err != nil {
			return false, errors.Wrap(err, "can't calculate hash of two concatenated hashes")
		}
	}

	return bytes.Equal(receipt.GetHeader().GetTxMerkelTreeRootHash(), currHash), nil
}

func VerifyLedgerPath(path []*types.BlockHeader) (bool, error) {
	if len(path) == 0 {
		return false, errors.New("impossible to validate empty path in ledger")
	}
	if len(path) == 1 {
		return false, errors.Errorf("ledger path composed from single block %d is impossible to validate", path[0].BaseHeader.Number)
	}
	currBlock := path[0]

	for i := 1; i < len(path) - 1 ; i++ {
		hashToLookFor, err := calculateHeaderHash(path[i])
		if err != nil {
			return false, err
		}
		hashFound := false
		for _, h := range currBlock.SkipchainHashes {
			if bytes.Equal(h, hashToLookFor) {
				hashFound = true
				break
			}
		}
		if !hashFound {
			return false, nil
		}
		currBlock = path[i]
	}

	return true, nil
}

func calculateHeaderHash(b *types.BlockHeader) ([]byte, error) {
	blockHeaderBytes, err := proto.Marshal(b)
	if err != nil {
		return nil, errors.Wrapf(err, "can't marshal block header {%d, %v}", b.BaseHeader.Number, b)
	}

	blockHash, err := crypto.ComputeSHA256Hash(blockHeaderBytes)
	if err != nil {
		return nil, errors.Wrapf(err, "can't calculate block hash {%d, %v}", b.BaseHeader.Number, b)
	}
	return blockHash, nil
}
