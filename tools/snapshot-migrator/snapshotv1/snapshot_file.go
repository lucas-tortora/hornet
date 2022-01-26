package snapshotv1

import (
	"encoding/binary"
	"fmt"
	"github.com/gohornet/hornet/pkg/model/hornet"
	"github.com/gohornet/hornet/pkg/model/milestone"
	"github.com/gohornet/hornet/pkg/model/utxo"
	"github.com/iotaledger/hive.go/serializer/v2"
	iotago "github.com/iotaledger/iota.go/v3"
	"github.com/pkg/errors"
	"io"
)

const (
	// The supported snapshot file version.
	SupportedFormatVersion byte = 1
)

var (
	// Returned when an output consumer has not been provided.
	ErrOutputConsumerNotProvided = errors.New("output consumer is not provided")
	// Returned when a treasury output consumer has not been provided.
	ErrTreasuryOutputConsumerNotProvided = errors.New("treasury output consumer is not provided")

	deSeriParas = &iotago.DeSerializationParameters{
		RentStructure: &iotago.RentStructure{
			VByteCost:    0,
			VBFactorData: 0,
			VBFactorKey:  0,
		},
	}
)

// Type defines the type of the snapshot.
type Type byte

const (
	// Full is a snapshot which contains the full ledger entry for a given milestone
	// plus the milestone diffs which subtracted to the ledger milestone reduce to the snapshot milestone ledger.
	Full Type = iota
	// Delta is a snapshot which contains solely diffs of milestones newer than a certain ledger milestone
	// instead of the complete ledger state of a given milestone.
	Delta
)

// maps the snapshot type to its name.
var snapshotNames = map[Type]string{
	Full:  "full",
	Delta: "delta",
}

// Output defines an output within a snapshot.
type Output struct {
	// The message ID of the message that contained the transaction where this output was created.
	MessageID [iotago.MessageIDLength]byte `json:"message_id"`
	// The transaction ID and the index of the output.
	OutputID [iotago.TransactionIDLength + 2]byte `json:"output_id"`
	// The type of the output.
	OutputType iotago.OutputType `json:"output_type"`
	// The underlying address to which this output deposits to.
	Address serializer.Serializable `json:"address"`
	// The amount of the deposit.
	Amount uint64 `json:"amount"`
}

// Spent defines a spent within a snapshot.
type Spent struct {
	Output
	// The transaction ID the funds were spent with.
	TargetTransactionID [iotago.TransactionIDLength]byte `json:"target_transaction_id"`
}

// MilestoneDiff represents the outputs which were created and consumed for the given milestone
// and the message itself which contains the milestone.
type MilestoneDiff struct {
	// The milestone payload itself.
	Milestone *iotago.Milestone `json:"milestone"`
	// The created outputs with this milestone.
	Created []*Output `json:"created"`
	// The consumed spents with this milestone.
	Consumed []*Spent `json:"consumed"`
	// The consumed treasury output with this milestone.
	SpentTreasuryOutput *utxo.TreasuryOutput
}

// TreasuryOutput extracts the new treasury output from within the milestone receipt.
// Might return nil if there is no receipt within the milestone.
func (md *MilestoneDiff) TreasuryOutput() *utxo.TreasuryOutput {
	if md.Milestone.Receipt == nil {
		return nil
	}
	to := md.Milestone.Receipt.(*iotago.Receipt).Transaction.Output
	msID, err := md.Milestone.ID()
	if err != nil {
		panic(err)
	}
	utxoTo := &utxo.TreasuryOutput{Amount: to.Amount}
	copy(utxoTo.MilestoneID[:], msID[:])
	return utxoTo
}

// SEPConsumerFunc consumes the given solid entry point.
// A returned error signals to cancel further reading.
type SEPConsumerFunc func(hornet.MessageID) error

// HeaderConsumerFunc consumes the snapshot file header.
// A returned error signals to cancel further reading.
type HeaderConsumerFunc func(*ReadFileHeader) error

// OutputConsumerFunc consumes the given output.
// A returned error signals to cancel further reading.
type OutputConsumerFunc func(output *Output) error

// UnspentTreasuryOutputConsumerFunc consumes the given treasury output.
// A returned error signals to cancel further reading.
type UnspentTreasuryOutputConsumerFunc func(output *utxo.TreasuryOutput) error

// MilestoneDiffConsumerFunc consumes the given MilestoneDiff.
// A returned error signals to cancel further reading.
type MilestoneDiffConsumerFunc func(milestoneDiff *MilestoneDiff) error

// FileHeader is the file header of a snapshot file.
type FileHeader struct {
	// Version denotes the version of this snapshot.
	Version byte
	// Type denotes the type of this snapshot.
	Type Type
	// The ID of the network for which this snapshot is compatible with.
	NetworkID uint64
	// The milestone index of the SEPs for which this snapshot was taken.
	SEPMilestoneIndex milestone.Index
	// The milestone index of the ledger data within the snapshot.
	LedgerMilestoneIndex milestone.Index
	// The treasury output existing for the given ledger milestone index.
	// This field must be populated if a Full snapshot is created/read.
	TreasuryOutput *utxo.TreasuryOutput
}

// ReadFileHeader is a FileHeader but with additional content read from the snapshot.
type ReadFileHeader struct {
	FileHeader
	// The time at which the snapshot was taken.
	Timestamp uint64
	// The count of solid entry points.
	SEPCount uint64
	// The count of outputs. This count is zero if a delta snapshot has been read.
	OutputCount uint64
	// The count of milestone diffs.
	MilestoneDiffCount uint64
}

// readSnapshotHeader reads the snapshot header from the given reader.
func readSnapshotHeader(reader io.Reader) (*ReadFileHeader, error) {
	readHeader := &ReadFileHeader{}

	if err := binary.Read(reader, binary.LittleEndian, &readHeader.Version); err != nil {
		return nil, fmt.Errorf("unable to read LS version: %w", err)
	}

	if err := binary.Read(reader, binary.LittleEndian, &readHeader.Type); err != nil {
		return nil, fmt.Errorf("unable to read LS type: %w", err)
	}

	if err := binary.Read(reader, binary.LittleEndian, &readHeader.Timestamp); err != nil {
		return nil, fmt.Errorf("unable to read LS timestamp: %w", err)
	}

	if err := binary.Read(reader, binary.LittleEndian, &readHeader.NetworkID); err != nil {
		return nil, fmt.Errorf("unable to read LS network ID: %w", err)
	}

	if err := binary.Read(reader, binary.LittleEndian, &readHeader.SEPMilestoneIndex); err != nil {
		return nil, fmt.Errorf("unable to read LS SEPs milestone index: %w", err)
	}

	if err := binary.Read(reader, binary.LittleEndian, &readHeader.LedgerMilestoneIndex); err != nil {
		return nil, fmt.Errorf("unable to read LS ledger milestone index: %w", err)
	}

	if err := binary.Read(reader, binary.LittleEndian, &readHeader.SEPCount); err != nil {
		return nil, fmt.Errorf("unable to read LS SEPs count: %w", err)
	}

	if readHeader.Type == Full {
		if err := binary.Read(reader, binary.LittleEndian, &readHeader.OutputCount); err != nil {
			return nil, fmt.Errorf("unable to read LS outputs count: %w", err)
		}
	}

	if err := binary.Read(reader, binary.LittleEndian, &readHeader.MilestoneDiffCount); err != nil {
		return nil, fmt.Errorf("unable to read LS ms-diff count: %w", err)
	}

	if readHeader.Type == Full {
		to := &utxo.TreasuryOutput{Spent: false}
		if _, err := io.ReadFull(reader, to.MilestoneID[:]); err != nil {
			return nil, fmt.Errorf("unable to read LS treasury output milestone hash: %w", err)
		}
		if err := binary.Read(reader, binary.LittleEndian, &to.Amount); err != nil {
			return nil, fmt.Errorf("unable to read LS treasury output amount: %w", err)
		}

		readHeader.TreasuryOutput = to
	}

	return readHeader, nil
}

// StreamSnapshotDataFrom consumes a snapshot from the given reader.
// OutputConsumerFunc must not be nil if the snapshot is not a delta snapshot.
func StreamSnapshotDataFrom(reader io.Reader,
	headerConsumer HeaderConsumerFunc,
	sepConsumer SEPConsumerFunc,
	outputConsumer OutputConsumerFunc,
	unspentTreasuryOutputConsumer UnspentTreasuryOutputConsumerFunc,
	msDiffConsumer MilestoneDiffConsumerFunc) error {

	readHeader, err := readSnapshotHeader(reader)
	if err != nil {
		return err
	}

	if readHeader.Type == Full {
		switch {
		case outputConsumer == nil:
			return ErrOutputConsumerNotProvided
		case unspentTreasuryOutputConsumer == nil:
			return ErrTreasuryOutputConsumerNotProvided
		}

		if err := unspentTreasuryOutputConsumer(readHeader.TreasuryOutput); err != nil {
			return err
		}
	}

	if err := headerConsumer(readHeader); err != nil {
		return err
	}

	for i := uint64(0); i < readHeader.SEPCount; i++ {
		solidEntryPointMessageID := make(hornet.MessageID, iotago.MessageIDLength)
		if _, err := io.ReadFull(reader, solidEntryPointMessageID); err != nil {
			return fmt.Errorf("unable to read LS SEP at pos %d: %w", i, err)
		}
		if err := sepConsumer(solidEntryPointMessageID); err != nil {
			return fmt.Errorf("SEP consumer error at pos %d: %w", i, err)
		}
	}

	if readHeader.Type == Full {
		for i := uint64(0); i < readHeader.OutputCount; i++ {
			output, err := readOutput(reader)
			if err != nil {
				return fmt.Errorf("at pos %d: %w", i, err)
			}

			if err := outputConsumer(output); err != nil {
				return fmt.Errorf("output consumer error at pos %d: %w", i, err)
			}
		}
	}

	for i := uint64(0); i < readHeader.MilestoneDiffCount; i++ {
		msDiff, err := readMilestoneDiff(reader)
		if err != nil {
			return fmt.Errorf("at pos %d: %w", i, err)
		}
		if err := msDiffConsumer(msDiff); err != nil {
			return fmt.Errorf("ms-diff consumer error at pos %d: %w", i, err)
		}
	}

	return nil
}

// reads a MilestoneDiff from the given reader.
func readMilestoneDiff(reader io.Reader) (*MilestoneDiff, error) {
	msDiff := &MilestoneDiff{}

	var msLength uint32
	if err := binary.Read(reader, binary.LittleEndian, &msLength); err != nil {
		return nil, fmt.Errorf("unable to read LS ms-diff ms length: %w", err)
	}

	msBytes := make([]byte, msLength)
	ms := &iotago.Milestone{}
	if _, err := io.ReadFull(reader, msBytes); err != nil {
		return nil, fmt.Errorf("unable to read LS ms-diff ms: %w", err)
	}

	if _, err := ms.Deserialize(msBytes, serializer.DeSeriModePerformValidation, deSeriParas); err != nil {
		return nil, fmt.Errorf("unable to deserialize LS ms-diff ms: %w", err)
	}

	msDiff.Milestone = ms

	if ms.Receipt != nil {
		spentTreasuryOutput := &utxo.TreasuryOutput{Spent: true}
		if _, err := io.ReadFull(reader, spentTreasuryOutput.MilestoneID[:]); err != nil {
			return nil, fmt.Errorf("unable to read LS ms-diff treasury input milestone hash: %w", err)
		}

		if err := binary.Read(reader, binary.LittleEndian, &spentTreasuryOutput.Amount); err != nil {
			return nil, fmt.Errorf("unable to read LS ms-diff treasury input milestone amount: %w", err)
		}

		msDiff.SpentTreasuryOutput = spentTreasuryOutput
	}

	var createdCount, consumedCount uint64
	if err := binary.Read(reader, binary.LittleEndian, &createdCount); err != nil {
		return nil, fmt.Errorf("unable to read LS ms-diff created count: %w", err)
	}

	msDiff.Created = make([]*Output, createdCount)
	for i := uint64(0); i < createdCount; i++ {
		diffCreatedOutput, err := readOutput(reader)
		if err != nil {
			return nil, fmt.Errorf("(ms-diff created-output) at pos %d: %w", i, err)
		}
		msDiff.Created[i] = diffCreatedOutput
	}

	if err := binary.Read(reader, binary.LittleEndian, &consumedCount); err != nil {
		return nil, fmt.Errorf("unable to read LS ms-diff consumed count: %w", err)
	}

	msDiff.Consumed = make([]*Spent, consumedCount)
	for i := uint64(0); i < consumedCount; i++ {
		diffConsumedSpent, err := readSpent(reader)
		if err != nil {
			return nil, fmt.Errorf("(ms-diff consumed-output) at pos %d: %w", i, err)
		}
		msDiff.Consumed[i] = diffConsumedSpent
	}

	return msDiff, nil
}

// reads an Output from the given reader.
func readOutput(reader io.Reader) (*Output, error) {
	output := &Output{}
	if _, err := io.ReadFull(reader, output.MessageID[:]); err != nil {
		return nil, fmt.Errorf("unable to read LS message ID: %w", err)
	}

	if _, err := io.ReadFull(reader, output.OutputID[:]); err != nil {
		return nil, fmt.Errorf("unable to read LS output ID: %w", err)
	}

	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(reader, typeBuf); err != nil {
		return nil, fmt.Errorf("unable to read LS output type: %w", err)
	}
	output.OutputType = iotago.OutputType(typeBuf[0])

	// look ahead address type
	var addrTypeBuf [serializer.SmallTypeDenotationByteSize]byte
	if _, err := io.ReadFull(reader, addrTypeBuf[:]); err != nil {
		return nil, fmt.Errorf("unable to read LS output address type byte: %w", err)
	}

	addrType := addrTypeBuf[0]
	addr, err := iotago.AddressSelector(uint32(addrType))
	if err != nil {
		return nil, fmt.Errorf("unable to determine address type of LS output: %w", err)
	}

	var addrDataWithoutType []byte
	switch addr.(type) {
	case *iotago.Ed25519Address:
		addrDataWithoutType = make([]byte, iotago.Ed25519AddressBytesLength)
	default:
		panic("unknown address type")
	}

	// read the rest of the address
	if _, err := io.ReadFull(reader, addrDataWithoutType); err != nil {
		return nil, fmt.Errorf("unable to read LS output address: %w", err)
	}

	if _, err := addr.Deserialize(append(addrTypeBuf[:], addrDataWithoutType...), serializer.DeSeriModePerformValidation, deSeriParas); err != nil {
		return nil, fmt.Errorf("invalid LS output address: %w", err)
	}
	output.Address = addr

	if err := binary.Read(reader, binary.LittleEndian, &output.Amount); err != nil {
		return nil, fmt.Errorf("unable to read LS output value: %w", err)
	}

	return output, nil
}

func readSpent(reader io.Reader) (*Spent, error) {
	output, err := readOutput(reader)
	if err != nil {
		return nil, err
	}

	spent := &Spent{Output: *output}
	if _, err := io.ReadFull(reader, spent.TargetTransactionID[:]); err != nil {
		return nil, fmt.Errorf("unable to read LS target transaction ID: %w", err)
	}

	return spent, nil
}
