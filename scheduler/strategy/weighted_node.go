package strategy

import (
	"github.com/docker/swarm/cluster"
	"github.com/docker/swarm/scheduler/node"
	log "github.com/Sirupsen/logrus"
)

// WeightedNode represents a node in the cluster with a given weight, typically used for sorting
// purposes.
type weightedNode struct {
	Node *node.Node
	// Weight is the inherent value of this node.
	Weight int64
}

type weightedNodeList []*weightedNode

func (n weightedNodeList) Len() int {
	return len(n)
}

func (n weightedNodeList) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

func (n weightedNodeList) Less(i, j int) bool {
	var (
		ip = n[i]
		jp = n[j]
	)

	// If the nodes have the same weight sort them out by number of containers.
	if ip.Weight == jp.Weight {
		return len(ip.Node.Containers) < len(jp.Node.Containers)
	}
	return ip.Weight < jp.Weight
}

func weighNodes(config *cluster.ContainerConfig, nodes []*node.Node, healthinessFactor int64) (weightedNodeList, error) {
	weightedNodes := weightedNodeList{}

	for _, node := range nodes {
		nodeMemory := node.TotalMemory
		nodeCpus := node.TotalCpus

		// Skip nodes that are smaller than the requested resources.
		if nodeMemory < int64(config.Memory) || nodeCpus < config.CpuShares {
			log.WithFields(log.Fields{ "swarmID": config.Labels["com.docker.swarm.id"], "nodeMemory": nodeMemory }).Debugf("XXXX: weighNodes SKIPPING")
			continue
		}

		var (
			cpuScore    int64 = 100
			memoryScore int64 = 100
		)

		if config.CpuShares > 0 {
			cpuScore = (node.UsedCpus + config.CpuShares) * 100 / nodeCpus
		}
		if config.Memory > 0 {
			memoryScore = (node.UsedMemory + config.Memory) * 100 / nodeMemory
		}

		if cpuScore <= 100 && memoryScore <= 100 {
			weightedNodes = append(weightedNodes, &weightedNode{Node: node, Weight: cpuScore + memoryScore + healthinessFactor*node.HealthIndicator})
		}
	}

	if len(weightedNodes) == 0 {
		for _, node := range nodes {
			nodeMemory := node.TotalMemory
			nodeCpus := node.TotalCpus

			// Skip nodes that are smaller than the requested resources.
			if nodeMemory < int64(config.Memory) || nodeCpus < config.CpuShares {
				continue
			}

			var (
				cpuScore    int64 = 100
				memoryScore int64 = 100
			)

			if config.CpuShares > 0 {
				cpuScore = (node.UsedCpus + config.CpuShares) * 100 / nodeCpus
			}
			if config.Memory > 0 {
				memoryScore = (node.UsedMemory + config.Memory) * 100 / nodeMemory
			}

			log.WithFields(log.Fields{ "swarmID": config.Labels["com.docker.swarm.id"], "cpuScore": cpuScore, "memoryScore": memoryScore, "IP": node.IP, "UsedMemory": node.UsedMemory, "UsedCpus": node.UsedCpus, "TotalMemory": node.TotalMemory, "TotalCpus": node.TotalCpus, "HealthIndicator": node.HealthIndicator, "numContainers": len(node.Containers) }).Debugf("XXXX: weighNodes loop")
		}

		log.WithFields(log.Fields{ "swarmID": config.Labels["com.docker.swarm.id"], "numNode": len(nodes), "reqMemory": config.Memory, "reqCpu": config.CpuShares }).Debugf("XXXX: ErrNoResourcesAvailable")
		return nil, ErrNoResourcesAvailable
	}

	return weightedNodes, nil
}
