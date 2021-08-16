// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/IBM-Blockchain/bcdb-server/pkg/cryptoservice"
	"go.uber.org/zap/zapcore"
	"net/http"
	"net/url"
	"time"

	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

const (
	contextTimeoutMargin = time.Second
)

type commonTxContext struct {
	userID        string
	signer        Signer
	userCert      []byte
	replicaSet    map[string]*url.URL
	verifier      SignatureVerifier
	restClient    RestClient
	txEnvelope    proto.Message
	commitTimeout time.Duration
	queryTimeout  time.Duration
	txSpent       bool
	logger        *logger.SugarLogger
}

type txContext interface {
	composeEnvelope(txID string) (proto.Message, error)
	// structs which embed the commonTXContext must implement this to clean the parts of the state that are not common.
	cleanCtx()
}

func (t *commonTxContext) commit(tx txContext, postEndpoint string, sync bool) (string, *types.TxReceipt, error) {
	if t.txSpent {
		return "", nil, ErrTxSpent
	}

	replica := t.selectReplica()
	postEndpointResolved := replica.ResolveReference(&url.URL{Path: postEndpoint})

	txID, err := computeTxID(t.userCert)
	if err != nil {
		return "", nil, err
	}

	t.logger.Debugf("compose transaction enveloped with txID = %s", txID)
	t.txEnvelope, err = tx.composeEnvelope(txID)
	if err != nil {
		t.logger.Errorf("failed to compose transaction envelope, due to %s", err)
		return txID, nil, err
	}
	ctx := context.Background()
	serverTimeout := time.Duration(0)
	if sync {
		serverTimeout = t.commitTimeout
		contextTimeout := t.commitTimeout + contextTimeoutMargin
		var cancelFnc context.CancelFunc
		ctx, cancelFnc = context.WithTimeout(context.Background(), contextTimeout)
		defer cancelFnc()
	}
	defer tx.cleanCtx()

	if t.logger.SugaredLogger.Desugar().Core().Enabled(zapcore.DebugLevel) {
		str, _ := json.MarshalIndent(t.txEnvelope, "", "  ")
		strNoIntent, _ := json.Marshal(t.txEnvelope)
		t.logger.Debugf("Envelope unintent :\n %s \n", string(strNoIntent))
		t.logger.Debugf("Envelope :\n %s \n", string(str))
	}

	response, err := t.restClient.Submit(ctx, postEndpointResolved.String(), t.txEnvelope, serverTimeout)
	if err != nil {
		t.logger.Errorf("failed to submit transaction txID = %s, due to %s", txID, err)
		return txID, nil, err
	}

	if response.StatusCode != http.StatusOK {
		var errMsg string
		if response.StatusCode == http.StatusAccepted {
			return txID, nil, &ServerTimeout{TxID: txID}
		}
		if response.Body != nil {
			errRes := &types.HttpResponseErr{}
			if err := json.NewDecoder(response.Body).Decode(errRes); err != nil {
				t.logger.Errorf("failed to parse the server's error message, due to %s", err)
				errMsg = "(failed to parse the server's error message)"
			} else {
				errMsg = errRes.Error()
			}
		}

		return txID, nil, errors.Errorf("failed to submit transaction, server returned: status: %s, message: %s", response.Status, errMsg)
	}

	txResponseEnvelope := &types.TxReceiptResponseEnvelope{}
	err = json.NewDecoder(response.Body).Decode(txResponseEnvelope)
	if err != nil {
		t.logger.Errorf("failed to decode json response, due to %s", err)
		return txID, nil, err
	}

	nodeID := txResponseEnvelope.GetResponse().GetHeader().GetNodeId()
	respBytes, err := json.Marshal(txResponseEnvelope.GetResponse())
	if err != nil {
		t.logger.Errorf("failed to marshal the response")
		return txID, nil, err
	}

	err = t.verifier.Verify(nodeID, respBytes, txResponseEnvelope.GetSignature())
	if err != nil {
		t.logger.Errorf("signature verification failed nodeID %s, due to %s", nodeID, err)
		return "", nil, errors.Errorf("signature verification failed nodeID %s, due to %s", nodeID, err)
	}

	t.txSpent = true
	tx.cleanCtx()
	return txID, txResponseEnvelope.GetResponse().GetReceipt(), nil
}

func (t *commonTxContext) abort(tx txContext) error {
	if t.txSpent {
		return ErrTxSpent
	}

	t.txSpent = true
	tx.cleanCtx()
	return nil
}

func (t *commonTxContext) selectReplica() *url.URL {
	// Pick first replica to send request to
	for _, replica := range t.replicaSet {
		return replica
	}
	return nil
}

func (t *commonTxContext) handleRequest(rawurl string, query, res proto.Message) error {
	parsedURL, err := url.Parse(rawurl)
	if err != nil {
		return err
	}
	restURL := t.selectReplica().ResolveReference(parsedURL).String()
	ctx := context.Background()
	if t.queryTimeout > 0 {
		contextTimeout := t.queryTimeout
		var cancelFnc context.CancelFunc
		ctx, cancelFnc = context.WithTimeout(context.Background(), contextTimeout)
		defer cancelFnc()
	}

	if t.logger.SugaredLogger.Desugar().Core().Enabled(zapcore.DebugLevel) {
		str, _ := json.MarshalIndent(query, "", "  ")
		strNoIdent, _ := json.Marshal(query)
		t.logger.Debugf("Query URL:%s\n", restURL)
		t.logger.Debugf("Query:%s \n", string(str))
		t.logger.Debugf("Raw Query:%s\n", string(strNoIdent))

		signature, _ := cryptoservice.SignQuery(t.signer, query)
		t.logger.Debugf("Signature: %s\n", base64.StdEncoding.EncodeToString(signature))
	}

	response, err := t.restClient.Query(ctx, restURL, query)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		var errMsg string
		if response.Body != nil {
			errRes := &types.HttpResponseErr{}
			if err := json.NewDecoder(response.Body).Decode(errRes); err != nil {
				t.logger.Errorf("failed to parse the server's error message, due to %s", err)
				errMsg = "(failed to parse the server's error message)"
			} else {
				errMsg = errRes.Error()
			}
		}
		return errors.Errorf("error handling request, server returned: status: %s, message: %s", response.Status, errMsg)
	}

	err = json.NewDecoder(response.Body).Decode(res)
	if err != nil {
		t.logger.Errorf("failed to decode json response, due to %s", err)
		return err
	}

	if t.logger.SugaredLogger.Desugar().Core().Enabled(zapcore.DebugLevel) {
		str, _ := json.MarshalIndent(res, "", "  ")
		t.logger.Debugf("Response:\n %s \n", string(str))
	}
	return nil
}

func (t *commonTxContext) TxEnvelope() (proto.Message, error) {
	if t.txEnvelope == nil {
		return nil, ErrTxNotFinalized
	}
	return t.txEnvelope, nil
}

var ErrTxNotFinalized = errors.New("can't access tx envelope, transaction not finalized")

type ServerTimeout struct {
	TxID string
}

func (e *ServerTimeout) Error() string {
	return "timeout occurred on server side while submitting transaction, converted to asynchronous completion, TxID: " + e.TxID
}
