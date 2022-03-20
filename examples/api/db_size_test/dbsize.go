// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"math/rand"
	"os"
	"time"
)

/*
	- Creating database 'db' with 3 indexes associated with it (name, age, gender)
	- Adding multiple key, value pairs to the database
	- Execute 6 valid queries and 1 invalid query
*/

type Person struct {
	Name string "json:name"
	Data []byte "json:data"
}


func main() {
	if err := executeDataWriteExample("../../util/config.yml"); err != nil {
		os.Exit(1)
	}
}

func executeDataWriteExample(configLocation string) error {
	session, err := prepareData(configLocation)
	if session == nil || err != nil {
		return err
	}

	err = clearData(session)
	if err != nil {
		return err
	}

	err = createDatabase(session)
	if err != nil {
		return err
	}

	err = insertData(session, 1000000, 3, 1024)
	if err != nil {
		return err
	}

	err = validQueries(session)
	if err != nil {
		return err
	}

	return nil
}

func prepareData(configLocation string) (bcdb.DBSession, error) {
	c, err := util.ReadConfig(configLocation)
	if err != nil {
		fmt.Printf(err.Error())
		return nil, err
	}

	logger, err := logger.New(
		&logger.Config{
			Level:         "info",
			OutputPath:    []string{"stdout"},
			ErrOutputPath: []string{"stderr"},
			Encoding:      "console",
			Name:          "bcdb-client",
		},
	)
	if err != nil {
		fmt.Printf(err.Error())
		return nil, err
	}

	conConf := &config.ConnectionConfig{
		ReplicaSet: c.ConnectionConfig.ReplicaSet,
		RootCAs:    c.ConnectionConfig.RootCAs,
		Logger:     logger,
	}

	fmt.Println("Opening connection to database, configuration: ", c.ConnectionConfig)
	db, err := bcdb.Create(conConf)
	if err != nil {
		fmt.Printf("Database connection creating failed, reason: %s\n", err.Error())
		return nil, err
	}

	sessionConf := &config.SessionConfig{
		UserConfig:   c.SessionConfig.UserConfig,
		TxTimeout:    c.SessionConfig.TxTimeout,
		QueryTimeout: c.SessionConfig.QueryTimeout}

	fmt.Println("Opening session to database, configuration: ", c.SessionConfig)
	session, err := db.Session(sessionConf)
	if err != nil {
		fmt.Printf("Database session creating failed, reason: %s\n", err.Error())
		return nil, err
	}

	return session, nil
}

func clearData(session bcdb.DBSession) error {
	fmt.Println("Opening database transaction")
	dbTx, err := session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creation failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Checking if database 'db' already exists")
	exists, err := dbTx.Exists("db")
	if err != nil {
		fmt.Printf("Checking the existence of database failed, reason: %s\n", err.Error())
		return err
	}
	if exists {
		fmt.Println("Deleting database 'db'")
		err = dbTx.DeleteDB("db")
		if err != nil {
			fmt.Printf("Deleting database failed, reason: %s\n", err.Error())
			return err
		}
	}

	fmt.Println("Committing transaction")
	txID, _, err := dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	return nil
}

func createDatabase(session bcdb.DBSession) error {
	fmt.Println("Opening database transaction")
	dbTx, err := session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creation failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Creating database 'db' with name as index")
	index := map[string]types.IndexAttributeType{
		"name":   types.IndexAttributeType_STRING,
	}
	err = dbTx.CreateDB("db", index)
	if err != nil {
		fmt.Printf("Database creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing transaction")
	txID, _, err := dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	return nil
}

func insertData(session bcdb.DBSession, size, putsNum, entrySize int) error {
	for i := 0; i < size; i++ {
		tx, err := session.DataTx()
		if err != nil {
			fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
			return err
		}
		for j := 0; j < putsNum; j++ {
			v := prepareDataEntry(fmt.Sprintf("name_%d_%d", i, j), entrySize)
			k := fmt.Sprintf("%d_%d", i, j)
			vBytes, err := json.Marshal(v)
			if err != nil {
				fmt.Printf("Can't marshal %s, reason: %s\n", v, err.Error())
				return err
			}
			err = tx.Put("db", k, vBytes, nil)
			if err != nil {
				fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
				return err
			}
		}
		if i%250 == 0 {
			fmt.Printf("Committing transaction %d\n", i)
		}
		_, _, err = tx.Commit(false)
		if err != nil {
			fmt.Printf("Commit failed, reason: %s\n", err.Error())
			return err
		}
		time.Sleep(4*time.Millisecond)
	}
	return nil
}

func validQueries(session bcdb.DBSession) error {
	q, err := session.JSONQuery()
	if err != nil {
		fmt.Printf("Failed to return handler to access bcdb data through JSON query, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("")
	fmt.Println("=== query1 ===")
	query1 := `
	{
		"selector": {
			"name": {
				"$eq": "name_100_0"
			}
		}
	}
	`
	kvs, err := q.Execute("db", query1)
	if err != nil {
		fmt.Printf("Failed to execute JSON query, reason: %s\n", err.Error())
		return err
	}
	if kvs == nil {
		fmt.Println("kvs nil")
		return errors.New("kvs nil")
	}
	if len(kvs) != 1 {
		fmt.Println("Query results are not as expected")
		return errors.New("Query results are not as expected")
	}
	fmt.Printf("As expected the keys that returned are: %v\n", kvs)

	return nil
}

var abc = []byte("abcdefghijklnmopqrstuvwxyz")

func prepareDataEntry(name string, size int) *Person {
	d := &Person{Name: name}
	nameSize := len(name)
	dataSize := size - nameSize
	d.Data = make([]byte, dataSize)
	for i := 0; i < dataSize; i++ {
		rand.Intn(len(abc))
		d.Data[i] = abc[rand.Intn(len(abc))]
	}
	return d
}
