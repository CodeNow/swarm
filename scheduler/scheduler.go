package scheduler

import (
	"errors"
	"strings"
	"sync"

	"github.com/docker/swarm/cluster"
	"github.com/docker/swarm/scheduler/filter"
	"github.com/docker/swarm/scheduler/node"
	"github.com/docker/swarm/scheduler/strategy"
	log "github.com/Sirupsen/logrus"
)

var (
	errNoNodeAvailable = errors.New("No nodes available in the cluster")
)

// Scheduler is exported
type Scheduler struct {
	sync.Mutex

	strategy strategy.PlacementStrategy
	filters  []filter.Filter
}

// New is exported
func New(strategy strategy.PlacementStrategy, filters []filter.Filter) *Scheduler {
	return &Scheduler{
		strategy: strategy,
		filters:  filters,
	}
}

// SelectNodesForContainer will return a list of nodes where the container can
// be scheduled, sorted by order or preference.
func (s *Scheduler) SelectNodesForContainer(nodes []*node.Node, config *cluster.ContainerConfig) ([]*node.Node, error) {
	candidates, err := s.selectNodesForContainer(nodes, config, true)

	if err != nil {
		log.WithFields(log.Fields{ "swarmID": config.Labels["com.docker.swarm.id"], "numNode": len(nodes) }).Debugf("XXXX: SelectNodesForContainer part 1")
		candidates, err = s.selectNodesForContainer(nodes, config, false)
	}

	if err != nil {
		log.WithFields(log.Fields{ "swarmID": config.Labels["com.docker.swarm.id"], "numNode": len(nodes) }).Debugf("XXXX: SelectNodesForContainer part 2")
	}
	return candidates, err
}

func (s *Scheduler) selectNodesForContainer(nodes []*node.Node, config *cluster.ContainerConfig, soft bool) ([]*node.Node, error) {
	accepted, err := filter.ApplyFilters(s.filters, config, nodes, soft)
	log.WithFields(log.Fields{ "swarmID": config.Labels["com.docker.swarm.id"], "numAccepted": len(accepted) }).Debugf("XXXX: selectNodesForContainer filtered")
	if err != nil {
		return nil, err
	}

	if len(accepted) == 0 {
		return nil, errNoNodeAvailable
	}

	return s.strategy.RankAndSort(config, accepted)
}

// Strategy returns the strategy name
func (s *Scheduler) Strategy() string {
	return s.strategy.Name()
}

// Filters returns the list of filter's name
func (s *Scheduler) Filters() string {
	filters := []string{}
	for _, f := range s.filters {
		filters = append(filters, f.Name())
	}

	return strings.Join(filters, ", ")
}
