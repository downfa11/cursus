package coordinator

import "sort"

func (c *Coordinator) rebalanceRange(groupName string) {
	group := c.groups[groupName]
	if group == nil {
		return
	}

	members := make([]string, 0, len(group.Members))
	for id := range group.Members {
		members = append(members, id)
	}
	sort.Strings(members)

	if len(members) == 0 {
		return
	}

	partitionsPerConsumer := len(group.Partitions) / len(members)
	remainder := len(group.Partitions) % len(members)

	partitionIdx := 0
	for i, memberID := range members {
		count := partitionsPerConsumer
		if i < remainder {
			count++
		}
		group.Members[memberID].Assignments = group.Partitions[partitionIdx : partitionIdx+count]
		partitionIdx += count
	}
}
