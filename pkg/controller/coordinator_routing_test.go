package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type coordinatorRoutingRaftManager struct {
	MockRaftManagerForForward
	brokerFSM *fsm.BrokerFSM
}

type coordinatorRoutingTopicHandler struct {
	coordinator.TopicHandler
}

func (coordinatorRoutingTopicHandler) CreateTopic(string, int, bool, bool) error {
	return nil
}

func (m *coordinatorRoutingRaftManager) GetFSM() *fsm.BrokerFSM {
	return m.brokerFSM
}

func (m *coordinatorRoutingRaftManager) ApplyCommand(prefix string, data []byte) error {
	result := m.brokerFSM.Apply(&raft.Log{Data: append([]byte(prefix+":"), data...)})
	if err, ok := result.(error); ok {
		return err
	}
	return nil
}

func newCoordinatorRoutingHandler(
	brokerID string,
	brokerFSM *fsm.BrokerFSM,
	groupCoordinator *coordinator.Coordinator,
) *CommandHandler {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	rm := &coordinatorRoutingRaftManager{brokerFSM: brokerFSM}
	router := clusterController.NewClusterRouter(
		brokerID,
		"127.0.0.1:7000",
		nil,
		rm,
		cfg.BrokerPort,
		cfg.AdvertisedClientHost,
		cfg,
	)
	cluster := &clusterController.ClusterController{RaftManager: rm, Router: router}
	return NewCommandHandler(nil, cfg, groupCoordinator, nil, cluster)
}

func registerRoutingBroker(t *testing.T, brokerFSM *fsm.BrokerFSM, id string) {
	t.Helper()
	result := brokerFSM.Apply(&raft.Log{
		Index: 1,
		Data:  []byte(`REGISTER:{"id":"` + id + `","addr":"127.0.0.1:7000","status":"active"}`),
	})
	require.Nil(t, result)
}

func TestCheckCoordinatorNormalLocalAndRemoteRoutes(t *testing.T) {
	brokerFSM := fsm.NewBrokerFSM(nil, nil)
	registerRoutingBroker(t, brokerFSM, "node-1")

	t.Run("local", func(t *testing.T) {
		handler := newCoordinatorRoutingHandler("node-1", brokerFSM, nil)
		addr, isCoordinator, err := handler.checkCoordinator("workers")
		require.NoError(t, err)
		require.True(t, isCoordinator)
		require.Equal(t, AdvertisedAddr{}, addr)
	})

	t.Run("remote cached address", func(t *testing.T) {
		handler := newCoordinatorRoutingHandler("node-2", brokerFSM, nil)
		expected := AdvertisedAddr{Host: "broker-1.example", Port: 9100}
		handler.coordCache["node-1"] = coordCacheEntry{addr: expected, updated: time.Now()}

		addr, isCoordinator, err := handler.checkCoordinator("workers")
		require.NoError(t, err)
		require.False(t, isCoordinator)
		require.Equal(t, expected, addr)
	})
}

func TestPreparedTransactionRecoveryMovesToNewShardOwner(t *testing.T) {
	brokerFSM := fsm.NewBrokerFSM(nil, nil)
	registerRoutingBroker(t, brokerFSM, "node-a")
	registerRoutingBroker(t, brokerFSM, "node-b")
	handler := newCoordinatorRoutingHandler("node-b", brokerFSM, nil)

	txnID := ""
	var oldEpoch int64
	for i := 0; i < 10_000; i++ {
		candidate := fmt.Sprintf("takeover-%d", i)
		ownership, ok := brokerFSM.GetTransactionCoordinator(candidate)
		if ok && ownership.Owner == "node-a" {
			txnID = candidate
			oldEpoch = ownership.Epoch
			break
		}
	}
	require.NotEmpty(t, txnID)

	producerID, producerEpoch, err := handler.TxnManager.InitProducerWithMode(txnID, transaction.ModeProcessingV1)
	require.NoError(t, err)
	require.NoError(t, handler.TxnManager.SetCoordinatorEpoch(txnID, oldEpoch))
	require.NoError(t, handler.TxnManager.Begin(txnID, producerID, producerEpoch))
	_, err = handler.TxnManager.PrepareCommit(txnID, producerID, producerEpoch)
	require.NoError(t, err)

	prepared := handler.TxnManager.ExportState()[txnID]
	payload, err := json.Marshal(map[string]interface{}{
		"transaction":       prepared,
		"coordinator_owner": "node-a",
		"coordinator_epoch": oldEpoch,
	})
	require.NoError(t, err)
	require.Nil(t, brokerFSM.Apply(&raft.Log{Data: append([]byte("TXN_SYNC:"), payload...)}))

	result := brokerFSM.Apply(&raft.Log{Data: []byte(`DEREGISTER:{"id":"node-a"}`)})
	require.Nil(t, result)
	newOwnership, ok := brokerFSM.GetTransactionCoordinator(txnID)
	require.True(t, ok)
	require.Equal(t, "node-b", newOwnership.Owner)
	require.Greater(t, newOwnership.Epoch, oldEpoch)

	require.NoError(t, handler.RecoverPreparedTransactions())
	status, err := handler.TxnManager.Status(txnID)
	require.NoError(t, err)
	require.Equal(t, transaction.StateCommitted, status.State)
	require.Equal(t, newOwnership.Epoch, status.CoordinatorEpoch)
}

func TestGroupCommandsFailClosedWhenCoordinatorDiscoveryFails(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	groupCoordinator := coordinator.NewCoordinator(context.Background(), cfg, &coordinatorRoutingTopicHandler{})
	require.NoError(t, groupCoordinator.RegisterGroup("orders", "workers", 1))
	_, err := groupCoordinator.AddConsumer("workers", "worker-1")
	require.NoError(t, err)

	member := groupCoordinator.GetGroup("workers").Members["worker-1"]
	lastHeartbeat := member.LastHeartbeat
	handler := newCoordinatorRoutingHandler("node-2", nil, groupCoordinator)

	_, isCoordinator, err := handler.checkCoordinator("workers")
	require.ErrorContains(t, err, "FSM not available")
	require.False(t, isCoordinator)

	commands := []string{
		"JOIN_GROUP topic=orders group=workers member=new-worker",
		"SYNC_GROUP topic=orders group=workers member=worker-1",
		"LEAVE_GROUP topic=orders group=workers member=worker-1 generation=1",
		"FETCH_OFFSET topic=orders partition=0 group=workers",
		"GROUP_STATUS group=workers",
		"HEARTBEAT topic=orders group=workers member=worker-1",
		"COMMIT_OFFSET topic=orders partition=0 group=workers offset=1",
		"BATCH_COMMIT topic=orders group=workers generation=1 member=worker-1 P0:1",
	}
	for _, command := range commands {
		t.Run(command, func(t *testing.T) {
			response := handler.HandleCommand(command, NewClientContext("workers", 0))
			require.Equal(t, coordinatorUnavailableResponse, response)
		})
	}

	require.Equal(t, lastHeartbeat, member.LastHeartbeat)
}
