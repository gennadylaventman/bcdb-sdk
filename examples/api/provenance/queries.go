package main

import (
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"github.com/IBM-Blockchain/bcdb-sdk/pkg/bcdb"
	"github.com/IBM-Blockchain/bcdb-sdk/pkg/config"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/state"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"io/ioutil"
)

var tx = &types.DataTxEnvelope{
	Payload: &types.DataTx{
		MustSignUserIds: []string{"alice"},
		TxId:            "Tx000",
		DbOperations:    []*types.DBOperation{
			{
				DbName:      "db2",
				DataReads:   nil,
				DataWrites:  []*types.DataWrite{
					{
						Key:   "key1",
						Value: []byte("this is a first value"),
						Acl:   &types.AccessControl{
							ReadUsers:          usersMap("bob"),
							ReadWriteUsers:     usersMap("bob"),
						},
					},
				},
				DataDeletes: nil,
			},
		},
	},
	Signatures: map[string][]byte{"alice":[]byte("wrong signature")},
}

func main() {
	c, err := ReadConfig("./config.yml")
	if err != nil {
		fmt.Printf(err.Error())
	}

	logger, err := logger.New(
		&logger.Config{
			Level:         "debug",
			OutputPath:    []string{"stdout"},
			ErrOutputPath: []string{"stderr"},
			Encoding:      "console",
			Name:          "bcdb-client",
		},
	)

	conConf := &config.ConnectionConfig{
		ReplicaSet: c.ConnectionConfig.ReplicaSet,
		RootCAs:    c.ConnectionConfig.RootCAs,
		Logger:     logger,
	}

	db, err := bcdb.Create(conConf)
	if err != nil {
		fmt.Printf(err.Error())
	}

	session, err := db.Session(&c.AdminConfig)
	if err != nil {
		fmt.Printf(err.Error())
	}

//	initUsers(session)

	session, err = db.Session(&c.SessionConfig)
	if err != nil {
		fmt.Printf(err.Error())
	}

//	initData(session)

	//provenance, err := session.Provenance()
	//if err != nil {
	//	fmt.Printf(err.Error())
	//}

	//logger.Info("GetHistoricalData bdb key1")
	//provenance.GetHistoricalData("bdb", "key1")
	//
	//logger.Info("GetNextHistoricalData bdb key2 block 4 tx 1")
	//provenance.GetNextHistoricalData("bdb", "key2", &types.Version{
	//	BlockNum: 4,
	//	TxNum:    0,
	//})
	//
	//logger.Info("GetPreviousHistoricalData bdb key3 block 5 tx 1")
	//provenance.GetPreviousHistoricalData("bdb", "key3", &types.Version{
	//	BlockNum: 5,
	//	TxNum:    0,
	//})

	//logger.Info("GetHistoricalDataAt db2 key1 block 5 tx 0")
	//provenance.GetHistoricalDataAt("db2", "key1", &types.Version{
	//	BlockNum: 6,
	//	TxNum:    0,
	//})

	//logger.Info("GetTxIDsSubmittedByUser alice")
	//provenance.GetTxIDsSubmittedByUser("alice")

	//logger.Info("GetDataReadByUser alice")
	//provenance.GetDataReadByUser("alice")
	//
	//logger.Info("GetDataWrittenByUser alice")
	//provenance.GetDataWrittenByUser("alice")

	//logger.Info("GetReaders db2 key1")
	//provenance.GetReaders("db2", "key1")
	//
	//logger.Info("GetWriters db2 key2")
	//provenance.GetWriters("db2", "key2")
	//

	//db, err := bcdb.Create(conConf)
	//if err != nil {
	//	fmt.Printf(err.Error())
	//}
	//
	//session, err := db.Session(&c.SessionConfig)
	//if err != nil {
	//	fmt.Printf(err.Error())
	//}

	//tx, err := session.DataTx()
	//if err != nil {
	//	return
	//}
	//err = tx.Put("bdb", "key1", []byte("hello world"), nil)
	//if err != nil {
	//	return
	//}
	//
	//txId, txReceip, err := tx.Commit(true)
	//if err != nil {
	//	return
	//}
	//
	//fmt.Println("Submitted tx " + txId + " got receipt " + txReceip.String())

	ledger, err := session.Ledger()
	if err != nil {
		fmt.Printf(err.Error())
	}


	logger.Info("GetBlockHeader for block 5")
	b5h, err := ledger.GetBlockHeader(5)
	b6h, err := ledger.GetBlockHeader(6)
	//
	logger.Info("GetTransactionReceipt for tx Tx000")
	txReceipt, err := ledger.GetTransactionReceipt("Tx000")

	logger.Info("GetLedgerPath between blocks 1 and 6")
	path, err := ledger.GetLedgerPath(1, 6)
	res, err := bcdb.VerifyLedgerPath(path)
	logger.Infof("Ledger path verify result %t", res)

	logger.Info("GetTransactionProof for tx at index 0 in block 5")
	txProof, err := ledger.GetTransactionProof(5, 0)
	res, err = txProof.Verify(txReceipt, tx)
	logger.Infof("Data tx verify result %t", res)

	//	fmt.Printf("%s\n", string(resBytes))

	// Getting dat proof
	dataProof, err := ledger.GetDataProof(5, "db2", "key2", false)
	// Calculating <key, value> pair hash
	compositeKey, err := state.ConstructCompositeKey("db2", "key2")
	kvHash, err := state.CalculateKeyValueHash(compositeKey, []byte("this is a second value"))
	logger.Infof("Verifying %s", base64.StdEncoding.EncodeToString(kvHash))
	// Validation proof
	res, err = dataProof.Verify(kvHash, b5h.GetStateMerkelTreeRootHash(), false)
	logger.Infof("Verify result %t", res)

	dataProof, err = ledger.GetDataProof(6, "db2", "key1", false)
	compositeKey, err = state.ConstructCompositeKey("db2", "key1")
	kvHash, err = state.CalculateKeyValueHash(compositeKey, []byte("this is a first value updated"))
	res, err = dataProof.Verify(kvHash, b6h.GetStateMerkelTreeRootHash(), false)
	logger.Infof("Verifying %s", base64.StdEncoding.EncodeToString(kvHash))
	logger.Infof("Verify result %t", res)
}

func usersMap(users ...string) map[string]bool {
	m := make(map[string]bool)
	for _, u := range users {
		m[u] = true
	}
	return m
}

func initUsers(session bcdb.DBSession) {
	//dbTx, err := session.DBsTx()
	//if err != nil {
	//	fmt.Printf("Database transaction creating failed, reason: %s\n", err.Error())
	//	return
	//}
	//
	//fmt.Println("Creating new database db1")
	//err = dbTx.CreateDB("db1")
	//if err != nil {
	//	fmt.Printf("New database creating failed, reason: %s\n", err.Error())
	//	return
	//}
	//fmt.Println("Creating new database db2")
	//err = dbTx.CreateDB("db2")
	//if err != nil {
	//	fmt.Printf("New database creating failed, reason: %s\n", err.Error())
	//	return
	//}
	//
	//fmt.Println("Committing transaction")
	//txID, _, err := dbTx.Commit(true)
	//if err != nil {
	//	fmt.Printf("Commit failed, reason: %s\n", err.Error())
	//	return
	//}
	//fmt.Printf("Transaction number %s committed successfully\n", txID)

 	fmt.Println("Opening user transaction")
	tx, err := session.UsersTx()
	if err != nil {
		fmt.Printf("User transaction creating failed, reason: %s\n", err.Error())
		return
	}

	dbPerm := map[string]types.Privilege_Access{
		"db1": types.Privilege_Read,
		"db2": types.Privilege_ReadWrite,
	}
	//reading and decoding alice's certificate
	alicePemUserCert, err := ioutil.ReadFile("../../../../bcdb-server/sampleconfig/crypto/alice/alice.pem")
	if err != nil {
		fmt.Printf(err.Error())
		return
	}
	aliceCertBlock, _ := pem.Decode(alicePemUserCert)

	alice := &types.User{
		Id:          "alice",
		Certificate: aliceCertBlock.Bytes,
		Privilege: &types.Privilege{
			DbPermission: dbPerm,
		}}

	fmt.Println("Adding alice to the database")
	err = tx.PutUser(alice, nil)
	if err != nil {
		fmt.Printf("Adding new user to database failed, reason: %s\n", err.Error())
		return
	}

	dbPerm = map[string]types.Privilege_Access{
		"db1": types.Privilege_Read,
		"db2": types.Privilege_Read,
	}
	//reading and decoding bob's certificate
	bobPemUserCert, err := ioutil.ReadFile("../../../../bcdb-server/sampleconfig/crypto/bob/bob.pem")
	if err != nil {
		fmt.Printf(err.Error())
		return
	}
	bobCertBlock, _ := pem.Decode(bobPemUserCert)

	bob := &types.User{
		Id:          "bob",
		Certificate: bobCertBlock.Bytes,
		Privilege: &types.Privilege{
			DbPermission: dbPerm,
		}}

	fmt.Println("Adding bob to the database")
	err = tx.PutUser(bob, nil)
	if err != nil {
		fmt.Printf("Adding new user to database failed, reason: %s\n", err.Error())
		return
	}

	fmt.Println("Committing transaction")
	txID, _, err := tx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

}


func initData(session bcdb.DBSession) {
	dataInit, err := session.DataTx()
	if err != nil {
		fmt.Printf(err.Error())
	}

	if err = dataInit.Put("bdb", "key1", []byte("this is a first value"), &types.AccessControl{
		ReadUsers:          usersMap("bob"),
		ReadWriteUsers:     usersMap("bob"),
	}); err != nil {
		fmt.Printf(err.Error())
	}

	if err = dataInit.Put("bdb", "key2", []byte("this is a second value"), &types.AccessControl{
		ReadUsers:          usersMap("bob"),
		ReadWriteUsers:     usersMap("bob"),
	}); err != nil {
		fmt.Printf(err.Error())
	}

	if err = dataInit.Put("bdb", "key3", []byte("this is a third value"), &types.AccessControl{
		ReadUsers:          usersMap("bob"),
		ReadWriteUsers:     usersMap("bob"),
	}); err != nil {
		fmt.Printf(err.Error())
	}

	_, _, err = dataInit.Commit(true)
	if err != nil {
		fmt.Printf(err.Error())
	}

	dataUpdate, err := session.DataTx()
	if err != nil {
		fmt.Printf(err.Error())
	}

	k1Value, _, err := dataUpdate.Get("bdb", "key1")
	if err != nil {
		fmt.Printf(err.Error())
	}

	k2Value, _, err := dataUpdate.Get("bdb", "key2")
	if err != nil {
		fmt.Printf(err.Error())
	}

	if err = dataUpdate.Put("bdb", "key1", []byte(string(k1Value) + " updated"), &types.AccessControl{
		ReadUsers:          usersMap("bob"),
		ReadWriteUsers:     usersMap("bob"),
	}); err != nil {
		fmt.Printf(err.Error())
	}

	if err = dataUpdate.Put("bdb", "key2", []byte(string(k2Value) + " updated"), &types.AccessControl{
		ReadUsers:          usersMap("bob"),
		ReadWriteUsers:     usersMap("bob"),
	}); err != nil {
		fmt.Printf(err.Error())
	}

	if err = dataUpdate.Delete("bdb", "key3"); err != nil {
		fmt.Printf(err.Error())
	}

	_, _, err = dataUpdate.Commit(true)
	if err != nil {
		fmt.Printf(err.Error())
	}
}
