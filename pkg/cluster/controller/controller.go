package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/hashicorp/raft"
)

type RaftManager interface {
	IsLeader() bool
	GetLeaderAddress() string
	ApplyCommand(prefix string, data []byte) error
	LeaderCh() <-chan bool
	GetFSM() *fsm.BrokerFSM
	GetConfiguration() raft.ConfigurationFuture
	ReplicateWithQuorum(topic string, partition int, msg types.Message, minISR int) (types.AckResponse, error)
	ReplicateBatchWithQuorum(topic string, partition int, messages []types.Message, minISR int, acks string) (types.AckResponse, error)
	ApplyResponse(prefix string, data []byte, timeout time.Duration) (types.AckResponse, error)
}

type ClusterController struct {
	RaftManager RaftManager
	Discovery   ServiceDiscovery
	Election    *ControllerElection
	Router      *ClusterRouter
}

func NewClusterController(ctx context.Context, cfg *config.Config, rm RaftManager, sd ServiceDiscovery) *ClusterController {
	brokerID := fmt.Sprintf("%s-%d", cfg.AdvertisedHost, cfg.BrokerPort)
	localAddr := fmt.Sprintf("%s:%d", cfg.AdvertisedHost, cfg.BrokerPort)

	cc := &ClusterController{
		RaftManager: rm,
		Discovery:   sd,
		Election:    NewControllerElection(rm),
		Router:      NewClusterRouter(brokerID, localAddr, nil, rm, cfg.BrokerPort),
	}

	return cc
}

func (cc *ClusterController) Start(ctx context.Context) {
	cc.Election.Start()
	cc.Discovery.StartReconciler(ctx)
}

func (cc *ClusterController) SetLocalProcessor(lp LocalProcessor) {
	if lp == nil {
		util.Warn("LocalProcessor is nil, ignoring")
		return
	}
	if cc.Router != nil {
		cc.Router.localProcessor = lp
	}
}

func (cc *ClusterController) GetClusterLeader() (string, error) {
	leader := cc.RaftManager.GetLeaderAddress()
	if leader == "" {
		return "", fmt.Errorf("no cluster leader available")
	}
	return leader, nil
}

func (cc *ClusterController) JoinNewBroker(id, addr string) error {
	_, err := cc.Discovery.AddNode(id, addr)
	return err
}

func (cc *ClusterController) IsLeader() bool {
	if cc.RaftManager != nil {
		return cc.RaftManager.IsLeader()
	}
	util.Warn("RaftManager is nil, assuming non-leader state")
	return false
}

// IsAuthorized checks if this broker is the leader for the given partition.
// Falls back to Raft leader check if partition metadata is not yet available.
func (cc *ClusterController) IsAuthorized(topic string, partition int) bool {
	if cc.RaftManager == nil {
		return false
	}

	fsmInstance := cc.RaftManager.GetFSM()
	if fsmInstance == nil {
		return cc.IsLeader()
	}

	key := fmt.Sprintf("%s-%d", topic, partition)
	meta := fsmInstance.GetPartitionMetadata(key)
	if meta == nil || meta.Leader == "" {
		// No per-partition metadata yet; fall back to Raft leader as partition owner.
		return cc.IsLeader()
	}

	// Compare partition leader with this broker's identity.
	if cc.Router != nil && meta.Leader == cc.Router.brokerID {
		return true
	}

	// Fallback: if this node is the Raft leader, it can serve any partition
	// until partition-level routing is fully implemented.
	return cc.IsLeader()
}
