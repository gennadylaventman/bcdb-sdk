// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"testing"
	"time"

	"github.com/IBM-Blockchain/bcdb-sdk/internal/test"
	sdkconfig "github.com/IBM-Blockchain/bcdb-sdk/pkg/config"
	"github.com/IBM-Blockchain/bcdb-server/config"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/server"
	"github.com/IBM-Blockchain/bcdb-server/pkg/server/testutils"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func SetupTestServer(t *testing.T, cryptoTempDir string) (*server.BCDBHTTPServer, uint32, uint32, error) {
	s, nodePort, peerPort, e := SetupTestServerWithParams(t, cryptoTempDir, 500*time.Millisecond, 1)
	return s, nodePort, peerPort, e
}

func StartTestServer(t *testing.T, s *server.BCDBHTTPServer) {
	err := s.Start()
	require.NoError(t, err)
	require.Eventually(t, func() bool { return s.IsLeader() == nil }, 30*time.Second, 100*time.Millisecond)
}

func SetupTestServerWithParams(t *testing.T, cryptoTempDir string, blockTime time.Duration, txPerBlock uint32) (*server.BCDBHTTPServer, uint32, uint32, error) {
	tempDir, err := ioutil.TempDir(os.TempDir(), "userTxContextTest")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	caCertPEM, err := ioutil.ReadFile(path.Join(cryptoTempDir, testutils.RootCAFileName+".pem"))
	require.NoError(t, err)
	require.NotNil(t, caCertPEM)

	nodePort, peerPort := test.GetPorts()

	server, err := server.New(&config.Configurations{
		LocalConfig: &config.LocalConfiguration{
			Server: config.ServerConf{
				Identity: config.IdentityConf{ID: "testNode1",
					CertificatePath: path.Join(cryptoTempDir, "server.pem"),
					KeyPath:         path.Join(cryptoTempDir, "server.key"),
				},
				Network: config.NetworkConf{
					Address: "127.0.0.1",
					Port:    nodePort,
				},
				Database: config.DatabaseConf{
					Name:            "leveldb",
					LedgerDirectory: path.Join(tempDir, "ledger"),
				},
				QueueLength: config.QueueLengthConf{
					Block:                     10,
					Transaction:               10,
					ReorderedTransactionBatch: 10,
				},
				LogLevel: "info",
			},
			BlockCreation: config.BlockCreationConf{
				MaxBlockSize:                1000000,
				MaxTransactionCountPerBlock: txPerBlock,
				BlockTimeout:                blockTime,
			},
			Replication: config.ReplicationConf{
				WALDir:  path.Join(tempDir, "raft", "wal"),
				SnapDir: path.Join(tempDir, "raft", "snap"),
				Network: config.NetworkConf{
					Address: "127.0.0.1",
					Port:    peerPort},
				TLS: config.TLSConf{Enabled: false},
			},
			Bootstrap: config.BootstrapConf{},
		},
		SharedConfig: &config.SharedConfiguration{
			Nodes: []config.NodeConf{
				{
					NodeID:          "testNode1",
					Host:            "127.0.0.1",
					Port:            nodePort,
					CertificatePath: path.Join(cryptoTempDir, "server.pem"),
				},
			},
			Consensus: &config.ConsensusConf{
				Algorithm: "raft",
				Members: []*config.PeerConf{
					{
						NodeId:   "testNode1",
						RaftId:   1,
						PeerHost: "127.0.0.1",
						PeerPort: peerPort,
					},
				},
				RaftConfig: &config.RaftConf{
					TickInterval:         "10ms",
					ElectionTicks:        10,
					HeartbeatTicks:       1,
					MaxInflightBlocks:    50,
					SnapshotIntervalSize: math.MaxInt64,
				},
			},
			CAConfig: config.CAConfiguration{RootCACertsPath: []string{path.Join(cryptoTempDir, testutils.RootCAFileName+".pem")}},
			Admin: config.AdminConf{
				ID:              "admin",
				CertificatePath: path.Join(cryptoTempDir, "admin.pem"),
			},
		},
	})
	return server, nodePort, peerPort, err
}

func createTestLogger(t *testing.T) *logger.SugarLogger {
	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
		Name:          "bcdb-client",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)
	require.NotNil(t, logger)
	return logger
}

func openUserSession(t *testing.T, bcdb BCDB, user string, tempDir string) DBSession {
	return openUserSessionWithQueryTimeout(t, bcdb, user, tempDir, 0)
}

func openUserSessionWithQueryTimeout(t *testing.T, bcdb BCDB, user string, tempDir string, queryTimeout time.Duration) DBSession {
	// New session with alice user context
	session, err := bcdb.Session(&sdkconfig.SessionConfig{
		UserConfig: &sdkconfig.UserConfig{
			UserID:         user,
			CertPath:       path.Join(tempDir, user+".pem"),
			PrivateKeyPath: path.Join(tempDir, user+".key"),
		},
		TxTimeout:    time.Second * 20,
		QueryTimeout: queryTimeout,
	})
	require.NoError(t, err)

	return session
}

func createDBInstance(t *testing.T, cryptoDir string, serverPort string) BCDB {
	// Create new connection
	bcdb, err := Create(&sdkconfig.ConnectionConfig{
		RootCAs: []string{path.Join(cryptoDir, testutils.RootCAFileName+".pem")},
		ReplicaSet: []*sdkconfig.Replica{
			{
				ID:       "testNode1",
				Endpoint: fmt.Sprintf("http://localhost:%s", serverPort),
			},
		},
	})
	require.NoError(t, err)

	return bcdb
}

func startServerConnectOpenAdminCreateUserAndUserSession(t *testing.T, testServer *server.BCDBHTTPServer, certTempDir string, user string) (BCDB, DBSession, DBSession) {
	StartTestServer(t, testServer)

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, certTempDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(certTempDir, user+".pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	addUser(t, user, adminSession, pemUserCert, dbPerm)
	userSession := openUserSession(t, bcdb, user, certTempDir)

	return bcdb, adminSession, userSession
}

type TxFinality int

const (
	TxFinalityCommitSync TxFinality = iota
	TxFinalityCommitAsync
	TxFinalityAbort
)

func assertTxFinality(t *testing.T, txFinality TxFinality, tx TxContext, userSession DBSession) {
	var txID string
	var err error

	switch txFinality {
	case TxFinalityCommitSync:
		txID, receipt, err := tx.Commit(true)
		require.NoError(t, err)
		require.True(t, len(txID) > 0)
		require.NotNil(t, receipt)
	case TxFinalityCommitAsync:
		txID, receipt, err := tx.Commit(false)
		require.NoError(t, err)
		require.True(t, len(txID) > 0)
		require.Nil(t, receipt)
		switch tx.(type) {
		case ConfigTxContext:
			// TODO remove once support for non data tx provenance added
			e, _ := tx.TxEnvelope()
			env := e.(*types.ConfigTxEnvelope)
			newConfig := env.GetPayload().GetNewConfig()
			require.Eventually(t, func() bool {
				// verify tx was successfully committed. "Get" works once per Tx.
				cfgTx, err := userSession.ConfigTx()
				if err != nil {
					return false
				}
				clusterConfig, err := cfgTx.GetClusterConfig()
				if err != nil || clusterConfig == nil {
					return false
				}
				return proto.Equal(newConfig, clusterConfig)
			}, 5*time.Second, 100*time.Millisecond)
		case DataTxContext:
			waitForTx(t, txID, userSession)
		case DBsTxContext:
			// TODO remove once support for non data tx provenance added
			e, _ := tx.TxEnvelope()
			env := e.(*types.DBAdministrationTxEnvelope)
			createdDBs := env.GetPayload().GetCreateDbs()
			deletedDBs := env.GetPayload().GetDeleteDbs()
			require.Eventually(t, func() bool {
				// verify tx was successfully committed. "Get" works once per Tx.
				res := true
				dbTx, err := userSession.DBsTx()
				if err != nil {
					return false
				}
				if len(createdDBs) > 0 {
					for _, db := range createdDBs {
						exists, err := dbTx.Exists(db)
						if err != nil {
							return false
						}
						res = res && exists
					}
				}
				if len(deletedDBs) > 0 {
					for _, db := range createdDBs {
						exists, err := dbTx.Exists(db)
						if err != nil {
							return false
						}
						res = res && !exists
					}
				}
				return res
			}, 30*time.Second, 100*time.Millisecond)

		case UsersTxContext:
			// TODO remove once support for non data tx provenance added
			e, _ := tx.TxEnvelope()
			env := e.(*types.UserAdministrationTxEnvelope)
			deleteUsers := env.GetPayload().GetUserDeletes()
			updateUsers := env.GetPayload().GetUserWrites()
			require.Eventually(t, func() bool {
				// verify tx was successfully committed. "Get" works once per Tx.
				res := true
				userTx, err := userSession.UsersTx()
				if err != nil {
					return false
				}
				if len(deleteUsers) > 0 {
					for _, userDelete := range deleteUsers {
						userDelete.GetUserId()
						dUser, err := userTx.GetUser(userDelete.GetUserId())
						if err != nil {
							return false
						}
						res = res && (dUser == nil)
					}
				}
				if len(updateUsers) > 0 {
					for _, userUpdate := range updateUsers {
						uUser, err := userTx.GetUser(userUpdate.User.Id)
						if err != nil {
							return false
						}
						res = res && proto.Equal(uUser, userUpdate.User)
					}
				}
				return res
			}, 30*time.Second, 100*time.Millisecond)
		}
	case TxFinalityAbort:
		err = tx.Abort()
		require.NoError(t, err)
	}

	// verify finality

	txID, _, err = tx.Commit(true)
	require.EqualError(t, err, ErrTxSpent.Error())
	require.True(t, len(txID) == 0)
	txID, _, err = tx.Commit(false)
	require.EqualError(t, err, ErrTxSpent.Error())
	require.True(t, len(txID) == 0)

	err = tx.Abort()
	require.EqualError(t, err, ErrTxSpent.Error())
}

func MarshalOrPanic(response interface{}) []byte {
	bytes, err := json.Marshal(response)
	if err != nil {
		panic(err)
	}

	return bytes
}
