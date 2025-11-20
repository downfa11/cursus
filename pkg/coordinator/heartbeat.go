package coordinator

import (
	"log"
	"time"
)

func (c *Coordinator) monitorHeartbeats() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		for groupName, group := range c.groups {
			for memberID, member := range group.Members {
				timeout := time.Duration(c.cfg.SessionTimeoutMS) * time.Millisecond
				timeSinceLastHeartbeat := time.Since(member.LastHeartbeat)

				if timeSinceLastHeartbeat > timeout {
					log.Printf("[HEARTBEAT_TIMEOUT] ⚠️ Consumer '%s' in group '%s' timed out (last heartbeat: %v ago, timeout: %v)",
						memberID, groupName, timeSinceLastHeartbeat, timeout)
					log.Printf("[REBALANCE_TRIGGER] 🔄 Triggering rebalance for group '%s' due to consumer '%s' timeout",
						groupName, memberID)

					delete(group.Members, memberID)
					c.triggerRebalance(groupName)

					log.Printf("[MEMBER_REMOVED] ❌ Consumer '%s' removed from group '%s'. Remaining members: %d",
						memberID, groupName, len(group.Members))
				} else {
					log.Printf("[HEARTBEAT_OK] ✅ Consumer '%s' in group '%s' is healthy (last heartbeat: %v ago)",
						memberID, groupName, timeSinceLastHeartbeat)
				}
			}
		}
		c.mu.Unlock()
	}
}

func (c *Coordinator) triggerRebalance(groupName string) {
	c.rebalanceRange(groupName)
}
