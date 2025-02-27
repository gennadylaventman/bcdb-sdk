// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package commands

import (
	"encoding/json"
	"fmt"

	"github.com/IBM-Blockchain/bcdb-sdk/pkg/bcdb"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/pkg/errors"
)

// MintRequest a dealer issues a mint-request for a car.
func MintRequest(demoDir, dealerID, carRegistration string, lg *logger.SugarLogger) (out string, err error) {
	lg.Debugf("dealer-ID: %s, Car: %s", dealerID, carRegistration)

	serverUrl, err := loadServerUrl(demoDir)
	if err != nil {
		return "", errors.Wrap(err, "error loading server URL")
	}

	db, err := createDBInstance(demoDir, serverUrl)
	if err != nil {
		return "", errors.Wrap(err, "error creating database instance")
	}

	session, err := createUserSession(demoDir, db, dealerID)
	if err != nil {
		return "", errors.Wrap(err, "error creating database session")
	}

	record := &MintRequestRecord{
		Dealer:          dealerID,
		CarRegistration: carRegistration,
	}
	key := record.Key()

	dataTx, err := session.DataTx()
	if err != nil {
		return "", errors.Wrap(err, "error creating data transaction")
	}

	recordBytes, _, err := dataTx.Get(CarDBName, key)
	if err != nil {
		return "", errors.Wrapf(err, "error getting MintRequest: %s", key)
	}
	if recordBytes != nil {
		return "", errors.Errorf("MintRequest already exists: %s", key)
	}

	recordBytes, err = json.Marshal(record)

	err = dataTx.Put(CarDBName, key, recordBytes,
		&types.AccessControl{
			ReadUsers:      bcdb.UsersMap("dmv"),
			ReadWriteUsers: bcdb.UsersMap(dealerID),
		},
	)
	if err != nil {
		return "", errors.Wrap(err, "error during data transaction")
	}

	txID, txReceipt, err := dataTx.Commit(true)
	if err != nil {
		return "", errors.Wrap(err, "error during transaction commit")
	}

	txEnv, err := dataTx.TxEnvelope()
	if err != nil {
		return "", errors.New("error getting transaction envelope")
	}

	lg.Infof("MintRequest committed successfully: %s", txID)

	err = saveTxEvidence(demoDir, txID, txEnv, txReceipt, lg)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("MintRequest: committed, txID: %s, Key: %s", txID, key), nil
}

// MintApprove the dmv reviews and approves the mint-request.
// creates a car record in the database with the dealer as owner.
func MintApprove(demoDir, dmvID, mintReqRecordKey string, lg *logger.SugarLogger) (out string, err error) {
	lg.Debugf("dmv-ID: %s, Record-key: %s", dmvID, mintReqRecordKey)

	serverUrl, err := loadServerUrl(demoDir)
	if err != nil {
		return "", errors.Wrap(err, "error loading server URL")
	}

	db, err := createDBInstance(demoDir, serverUrl)
	if err != nil {
		return "", errors.Wrap(err, "error creating database instance")
	}

	session, err := createUserSession(demoDir, db, dmvID)
	if err != nil {
		return "", errors.Wrap(err, "error creating database session")
	}

	mintReqRec := &MintRequestRecord{}

	dataTx, err := session.DataTx()
	if err != nil {
		return "", errors.Wrap(err, "error creating data transaction")
	}

	recordBytes, _, err := dataTx.Get(CarDBName, mintReqRecordKey)
	if err != nil {
		return "", errors.Wrapf(err, "error getting MintRequest: %s", mintReqRecordKey)
	}
	if recordBytes == nil {
		return "", errors.Errorf("MintRequest not found: %s", mintReqRecordKey)
	}

	if err = json.Unmarshal(recordBytes, mintReqRec); err != nil {
		return "", errors.Wrapf(err, "error unmarshaling data transaction value, key: %s", mintReqRecordKey)
	}

	if err = validateMintRequest(mintReqRecordKey, mintReqRec); err != nil {
		return "", errors.WithMessage(err, "MintRequest validation failed")
	}

	carRecord := &CarRecord{
		Owner:           mintReqRec.Dealer,
		CarRegistration: mintReqRec.CarRegistration,
	}
	carKey := carRecord.Key()

	carRecordBytes, _, err := dataTx.Get(CarDBName, carKey)
	if err != nil {
		return "", errors.Wrapf(err, "error getting Car: %s", carKey)
	}
	if carRecordBytes != nil {
		return "", errors.Errorf("Car already exists: %s", carKey)
	}

	carRecordBytes, err = json.Marshal(carRecord)
	if err != nil {
		return "", errors.Wrapf(err, "error marshaling car record: %s", carRecord)
	}

	err = dataTx.Put(CarDBName, carKey, carRecordBytes,
		&types.AccessControl{
			ReadUsers:      bcdb.UsersMap(mintReqRec.Dealer),
			ReadWriteUsers: bcdb.UsersMap(dmvID),
		},
	)
	if err != nil {
		return "", errors.Wrap(err, "error during data transaction")
	}

	txID, txReceipt, err := dataTx.Commit(true)
	if err != nil {
		return "", errors.Wrap(err, "error during transaction commit")
	}

	txEnv, err := dataTx.TxEnvelope()
	if err != nil {
		return "", errors.New("error getting transaction envelope")
	}

	lg.Infof("MintApprove committed successfully: %s", txID)

	err = saveTxEvidence(demoDir, txID, txEnv, txReceipt, lg)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("MintApprove: committed, txID: %s, Key: %s", txID, carKey), nil
}

// Any validation, including provenance
func validateMintRequest(mintReqRecordKey string, mintReqRec *MintRequestRecord) error {
	reqID := mintReqRecordKey[len(MintRequestRecordKeyPrefix):]
	if reqID != mintReqRec.RequestID() {
		return errors.Errorf("MintRequest content compromised: expected: %s != actual: %s", reqID, mintReqRec.RequestID())
	}
	return nil
}
