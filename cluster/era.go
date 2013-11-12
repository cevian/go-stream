package cluster

import (
	"github.com/cloudflare/go-stream/util/slog"
)

type Era interface {
	GetNodes() []Node
}

type SimpleEra struct {
	nodes []Node
}

func NewSimpleEra() *SimpleEra {
	return &SimpleEra{make([]Node, 0, 0)}
}

func (s *SimpleEra) Add(n Node) {
	s.nodes = append(s.nodes, n)
}

func (s *SimpleEra) GetNodes() []Node {
	return s.nodes
}

type WeightedEra struct {
	nodes   []Node
	nodeMap [MAX_WEIGHT]*WeightedNode
}

func NewWeightedEra() *WeightedEra {
	return &WeightedEra{make([]Node, 0, 0), [MAX_WEIGHT]*WeightedNode{}}
}

func (s *WeightedEra) Add(n Node) {
	s.nodes = append(s.nodes, n)
}

func (s *WeightedEra) GetNodes() []Node {
	return s.nodes
}

func (s *WeightedEra) GetNode(posit int) *WeightedNode {
	if posit < MAX_WEIGHT {
		return s.nodeMap[posit]
	}
	return s.nodeMap[0]
}

func (s *WeightedEra) NormalizeAndPopulateMap() {
	total := float32(0)
	scalar := float32(1)
	for _, n := range s.nodes {
		wn := n.(*WeightedNode)
		total += float32(wn.weight)
	}

	if total == 0 {
		slog.Errorf("Total Node Weight 0")
		return
	}

	slog.Debugf("Total Node Weight %f", total)
	if total < MAX_WEIGHT {
		// Scale weights up
		scalar = MAX_WEIGHT / total
		total = MAX_WEIGHT
	}

	lastPosit := 0
	for _, n := range s.nodes {
		wn := n.(*WeightedNode)
		wn.weight = uint32(((float32(wn.weight) * scalar) / total) * MAX_WEIGHT)
		slog.Debugf("New Weight %d", wn.weight)
		for i := lastPosit; uint32(i) < wn.weight && i < MAX_WEIGHT; i++ {
			s.nodeMap[i] = wn
			lastPosit++
		}
	}

	return
}
