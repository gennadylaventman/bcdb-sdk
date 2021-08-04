package main

import (
	"encoding/pem"
	"fmt"
	"github.com/IBM-Blockchain/bcdb-sdk/pkg/bcdb"
	"github.com/IBM-Blockchain/bcdb-sdk/pkg/config"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"io/ioutil"
)

func main() {
	c, err := ReadConfig("./config.yml")
	if err != nil {
		fmt.Printf(err.Error())
	}

	logger, err := logger.New(
		&logger.Config{
			Level:         c.ConnectionConfig.LogLevel,
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

	session, err := db.Session(&c.SessionConfig)
	if err != nil {
		fmt.Printf(err.Error())
	}

	usersTx, err := session.UsersTx()
	if err != nil {
		fmt.Printf(err.Error())
	}

	certFile, err := ioutil.ReadFile("../crypto/bob/bob.pem")
	if err != nil {
		logger.Errorf("error reading certificate of %s, due to %s", "bob", err)
		return
	}
	certBlock, _ := pem.Decode(certFile)
	err = usersTx.PutUser(
		&types.User{
			Id:          "bob",
			Certificate: certBlock.Bytes,
			Privilege: &types.Privilege{
				DbPermission: map[string]types.Privilege_Access{"bdb": 1},
			},
		}, &types.AccessControl{
			ReadWriteUsers: usersMap("admin"),
			ReadUsers:      usersMap("admin"),
		})
	if err != nil {
		logger.Errorf("error create user %s, due to %s", "bob", err)
		usersTx.Abort()
		return
	}

	_, _, err = usersTx.Commit(true)
	if err != nil {
		logger.Errorf("error create user %s, due to %s", "bob", err)
		return
	}
}

func usersMap(users ...string) map[string]bool {
	m := make(map[string]bool)
	for _, u := range users {
		m[u] = true
	}
	return m
}
