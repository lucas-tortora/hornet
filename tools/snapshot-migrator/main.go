package main

import (
	"fmt"
	"github.com/gohornet/hornet/pkg/model/milestone"
	"os"

	"snapshot-migrator/snapshotv1"

	"github.com/gohornet/hornet/pkg/model/hornet"
	"github.com/gohornet/hornet/pkg/model/utxo"
	"github.com/gohornet/hornet/pkg/snapshot"
	iotago "github.com/iotaledger/iota.go/v3"
)

type migrator struct {
	header *snapshotv1.ReadFileHeader

	treasuryOutput *utxo.TreasuryOutput

	seps   hornet.MessageIDs
	sepIdx int

	outputs   []*snapshotv1.Output
	outputIdx int

	msDiff    []*snapshotv1.MilestoneDiff
	msDiffIdx int

	ledgerMilestoneTimestamp uint64
}

func newMigrator() *migrator {
	return &migrator{
		header: nil,
		seps:   hornet.MessageIDs{},
	}
}

func (m *migrator) headerConsumer() snapshotv1.HeaderConsumerFunc {
	return func(header *snapshotv1.ReadFileHeader) error {
		if header.Version != snapshotv1.SupportedFormatVersion {
			return fmt.Errorf("invalid snapshot version %d", header.Version)
		}
		if header.Type != snapshotv1.Full {
			return fmt.Errorf("only full snapshots are supported")
		}
		m.header = header
		return nil
	}
}

func (m *migrator) headerv2() *snapshot.FileHeader {
	return &snapshot.FileHeader{
		Version:              snapshot.SupportedFormatVersion,
		Type:                 snapshot.Full,
		NetworkID:            m.header.NetworkID,
		SEPMilestoneIndex:    m.header.SEPMilestoneIndex,
		LedgerMilestoneIndex: m.header.LedgerMilestoneIndex,
		TreasuryOutput:       m.treasuryOutput,
	}
}

func (m *migrator) timestamp() uint64 {
	return m.header.Timestamp
}

func (m *migrator) ledgerMilestoneIndex() milestone.Index {
	return m.header.LedgerMilestoneIndex
}

func (m *migrator) sepConsumer() snapshotv1.SEPConsumerFunc {
	return func(msgID hornet.MessageID) error {
		m.seps = append(m.seps, msgID)
		return nil
	}
}

func (m *migrator) sepProducer() snapshot.SEPProducerFunc {
	return func() (hornet.MessageID, error) {
		if m.sepIdx < len(m.seps) {
			sep := m.seps[m.sepIdx]
			m.sepIdx++
			return sep, nil
		}
		return nil, nil
	}
}

func (m *migrator) outputConsumer() snapshotv1.OutputConsumerFunc {
	return func(output *snapshotv1.Output) error {
		m.outputs = append(m.outputs, output)
		return nil
	}
}

func (m *migrator) outputProducer() snapshot.OutputProducerFunc {
	return func() (*utxo.Output, error) {
		if m.outputIdx < len(m.outputs) {
			o := m.outputs[m.outputIdx]
			m.outputIdx++

			outputId := &iotago.OutputID{}
			copy(outputId[:], o.OutputID[:])

			return utxo.CreateOutput(outputId, hornet.MessageIDFromArray(o.MessageID), m.header.LedgerMilestoneIndex, m.ledgerMilestoneTimestamp, &iotago.ExtendedOutput{
				Amount: o.Amount,
				Conditions: iotago.UnlockConditions{
					&iotago.AddressUnlockCondition{
						Address: o.Address.(iotago.Address),
					},
				},
			}), nil
		}
		return nil, nil
	}
}

func (m *migrator) unspentTreasuryOutputConsumer() snapshotv1.UnspentTreasuryOutputConsumerFunc {
	return func(output *utxo.TreasuryOutput) error {
		m.treasuryOutput = output
		return nil
	}
}

func (m *migrator) msDiffConsumer() snapshotv1.MilestoneDiffConsumerFunc {
	return func(milestoneDiff *snapshotv1.MilestoneDiff) error {

		if milestoneDiff.Milestone.Index == uint32(m.header.LedgerMilestoneIndex) {
			m.ledgerMilestoneTimestamp = milestoneDiff.Milestone.Timestamp
		}

		m.msDiff = append(m.msDiff, milestoneDiff)
		return nil
	}
}

func (m *migrator) msDiffProducer() snapshot.MilestoneDiffProducerFunc {
	return func() (*snapshot.MilestoneDiff, error) {
		if m.msDiffIdx < len(m.msDiff) {
			diff := m.msDiff[m.msDiffIdx]
			m.msDiffIdx++

			created := utxo.Outputs{}
			for _, o := range diff.Created {
				outputId := &iotago.OutputID{}
				copy(outputId[:], o.OutputID[:])

				created = append(created, utxo.CreateOutput(outputId, hornet.MessageIDFromArray(o.MessageID), milestone.Index(diff.Milestone.Index), diff.Milestone.Timestamp, &iotago.ExtendedOutput{
					Amount: o.Amount,
					Conditions: iotago.UnlockConditions{
						&iotago.AddressUnlockCondition{
							Address: o.Address.(iotago.Address),
						},
					},
				}))
			}

			consumed := utxo.Spents{}
			for _, s := range diff.Consumed {

				outputId := &iotago.OutputID{}
				copy(outputId[:], s.OutputID[:])

				// We do not know when the output was created, so we mark it as created in this milestone
				output := utxo.CreateOutput(outputId, hornet.MessageIDFromArray(s.MessageID), milestone.Index(diff.Milestone.Index), diff.Milestone.Timestamp, &iotago.ExtendedOutput{
					Amount: s.Amount,
					Conditions: iotago.UnlockConditions{
						&iotago.AddressUnlockCondition{
							Address: s.Address.(iotago.Address),
						},
					},
				})
				transactionID := &iotago.TransactionID{}
				copy(transactionID[:], s.TargetTransactionID[:])
				consumed = append(consumed, utxo.NewSpent(output, transactionID, milestone.Index(diff.Milestone.Index), diff.Milestone.Timestamp))
			}

			return &snapshot.MilestoneDiff{
				Milestone:           diff.Milestone,
				Created:             created,
				Consumed:            consumed,
				SpentTreasuryOutput: diff.SpentTreasuryOutput,
			}, nil
		}
		return nil, nil
	}
}

func main() {

	filePath := "full_snapshot.bin"
	targetFilePath := "new_full_snapshot.bin"

	// read back written data and verify that it is equal
	snapshotFileRead, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}

	migrator := newMigrator()

	if err := snapshotv1.StreamSnapshotDataFrom(
		snapshotFileRead,
		migrator.headerConsumer(),
		migrator.sepConsumer(),
		migrator.outputConsumer(),
		migrator.unspentTreasuryOutputConsumer(),
		migrator.msDiffConsumer(),
	); err != nil {
		panic(err)
	}

	snapshotFileWrite, err := os.OpenFile(targetFilePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}

	if _, err := snapshot.StreamSnapshotDataTo(
		snapshotFileWrite,
		migrator.timestamp(),
		migrator.headerv2(),
		migrator.sepProducer(),
		migrator.outputProducer(),
		migrator.msDiffProducer(),
	); err != nil {
		panic(err)
	}
}
