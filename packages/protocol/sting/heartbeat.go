package sting

import (
	"encoding/binary"

	"github.com/gohornet/hornet/packages/model/milestone"
)

// Heartbeat contains information about a nodes current solid and pruned milestone index.
type Heartbeat struct {
	SolidMilestoneIndex  milestone.Index `json:"solid_milestone_index"`
	PrunedMilestoneIndex milestone.Index `json:"pruned_milestone_index"`
}

/// ParseHeartbeat parses the given message into a heartbeat.
func ParseHeartbeat(data []byte) *Heartbeat {
	return &Heartbeat{
		SolidMilestoneIndex:  milestone.Index(binary.BigEndian.Uint32(data[:4])),
		PrunedMilestoneIndex: milestone.Index(binary.BigEndian.Uint32(data[4:])),
	}
}