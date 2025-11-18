package coordinator

import "time"

func (c *Coordinator) monitorHeartbeats() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		for groupName, group := range c.groups {
			for memberID, member := range group.Members {
				timeout := time.Duration(c.cfg.SessionTimeoutMS) * time.Millisecond
				if time.Since(member.LastHeartbeat) > timeout {
					delete(group.Members, memberID)
					c.triggerRebalance(groupName)
				}
			}
		}
		c.mu.Unlock()
	}
}

func (c *Coordinator) triggerRebalance(groupName string) {
	c.rebalanceRange(groupName)
}
