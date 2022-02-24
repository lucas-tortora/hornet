package toolset

import (
	"context"
	"fmt"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"
	flag "github.com/spf13/pflag"

	"github.com/iotaledger/hive.go/configuration"
	"github.com/iotaledger/hive.go/objectstorage"
	"github.com/iotaledger/hive.go/serializer"
	iotago "github.com/iotaledger/iota.go/v2"

	databasecore "github.com/gohornet/hornet/core/database"
	"github.com/gohornet/hornet/pkg/common"
	"github.com/gohornet/hornet/pkg/dag"
	"github.com/gohornet/hornet/pkg/database"
	"github.com/gohornet/hornet/pkg/keymanager"
	"github.com/gohornet/hornet/pkg/model/coordinator"
	"github.com/gohornet/hornet/pkg/model/hornet"
	"github.com/gohornet/hornet/pkg/model/milestone"
	"github.com/gohornet/hornet/pkg/model/milestonemanager"
	"github.com/gohornet/hornet/pkg/model/storage"
	"github.com/gohornet/hornet/pkg/restapi"
	"github.com/gohornet/hornet/pkg/snapshot"
	"github.com/gohornet/hornet/pkg/utils"
	"github.com/gohornet/hornet/pkg/whiteflag"
)

var (
	ErrNoNewHistory = errors.New("no new history available")
)

const (
	// the amount of public keys in a milestone.
	CfgProtocolMilestonePublicKeyCount = "protocol.milestonePublicKeyCount"
	// the ed25519 public key of the coordinator in hex representation.
	CfgProtocolPublicKeyRanges = "protocol.publicKeyRanges"
	// the ed25519 public key of the coordinator in hex representation.
	CfgProtocolPublicKeyRangesJSON = "publicKeyRanges"

	TangleDatabaseDirectoryName = "tangle"
	UTXODatabaseDirectoryName   = "utxo"
)

var (
	permascam = "http://chronicle-00.chrysalis-mainnet.iotaledger.net/api/mainnet"
)

func startProfiling() {
	runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(5)

	bindAddr := "localhost:6060"

	go func() {
		println(fmt.Sprintf("You can now access the profiling server using: http://%s/debug/pprof/", bindAddr))

		// pprof Server for Debugging
		if err := http.ListenAndServe(bindAddr, nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
			println(fmt.Sprintf("Stopped profiling server due to an error (%s)", err))
		}
	}()
}

func getMilestoneMessageIDFromStorage(tangleStore *storage.Storage, msIndex milestone.Index) (hornet.MessageID, error) {

	cachedMs := tangleStore.CachedMilestoneOrNil(msIndex) // milestone +1
	if cachedMs == nil {
		return nil, fmt.Errorf("milestone not found! %d", msIndex)
	}
	defer cachedMs.Release(true) // milestone -1

	return cachedMs.Milestone().MessageID, nil
}

func getMilestoneMessageFromStorage(tangleStore *storage.Storage, milestoneMessageID hornet.MessageID) (*storage.Message, error) {

	cachedMessage := tangleStore.CachedMessageOrNil(milestoneMessageID) // message +1
	if cachedMessage == nil {
		return nil, fmt.Errorf("milestone not found! %s", milestoneMessageID.ToHex())
	}
	defer cachedMessage.Release(true) // message -1

	return cachedMessage.Message(), nil
}

// getStorageMilestoneRange returns the range of milestones that are found in the storage.
func getStorageMilestoneRange(tangleStore *storage.Storage) (milestone.Index, milestone.Index) {
	var msIndexStart milestone.Index = math.MaxUint32
	var msIndexEnd milestone.Index = 0

	tangleStore.ForEachMilestoneIndex(func(msIndex milestone.Index) bool {
		if msIndexStart > msIndex {
			msIndexStart = msIndex
		}
		if msIndexEnd < msIndex {
			msIndexEnd = msIndex
		}
		return true
	}, objectstorage.WithIteratorSkipCache(true))

	if msIndexStart == math.MaxUint32 {
		// no milestone found
		msIndexStart = 0
	}

	return msIndexStart, msIndexEnd
}

func verifyDatabase(ctx context.Context, tangleStore *storage.Storage, milestoneManager *milestonemanager.MilestoneManager) error {
	msIndexStart, msIndexEnd := getStorageMilestoneRange(tangleStore)
	if msIndexStart == msIndexEnd {
		return fmt.Errorf("no target database entries %d-%d", msIndexStart, msIndexEnd)
	}

	println(fmt.Sprintf("Milestone range target database: %d-%d", msIndexStart, msIndexEnd))

	checkMilestoneCone := func(ctx context.Context, tangleStore *storage.Storage, milestoneManager *milestonemanager.MilestoneManager, onNewMilestoneConeMsg func(*storage.CachedMessage), msIndex milestone.Index) error {

		// traversal stops if no more messages pass the given condition
		// Caution: condition func is not in DFS order
		condition := func(cachedMetadata *storage.CachedMetadata) (bool, error) { // meta +1
			defer cachedMetadata.Release(true) // meta -1

			// collect all msg that were referenced by that milestone
			referenced, at := cachedMetadata.Metadata().ReferencedWithIndex()

			if !referenced {
				// all existing messages in the database must be referenced by a milestone
				return false, fmt.Errorf("message was not referenced (msIndex: %d, msgID: %s)", msIndex, cachedMetadata.Metadata().MessageID().ToHex())
			}

			if at > msIndex {
				return false, fmt.Errorf("milestone cone inconsistent (msIndex: %d, referencedAt: %d)", msIndex, at)
			}

			if at < msIndex {
				// do not traverse messages that were referenced by an older milestonee
				return false, nil
			}

			// check if the message exists
			cachedMsg := tangleStore.CachedMessageOrNil(cachedMetadata.Metadata().MessageID())
			if cachedMsg == nil {
				return false, fmt.Errorf("message not found: %s", cachedMetadata.Metadata().MessageID().ToHex())
			}
			defer cachedMsg.Release(true)

			if onNewMilestoneConeMsg != nil {
				onNewMilestoneConeMsg(cachedMsg.Retain())
			}

			return true, nil
		}

		// we don't need to call cleanup at the end, because we pass our own metadataMemcache.
		syncParentsTraverser := dag.NewParentTraverser(tangleStore.CachedMessageMetadataOrNil, tangleStore.SolidEntryPointsContain, nil)

		milestoneMessageID, err := getMilestoneMessageIDFromStorage(tangleStore, msIndex)
		if err != nil {
			return err
		}

		// traverse the milestone and collect all messages that were referenced by this milestone or newer
		if err := syncParentsTraverser.Traverse(
			ctx,
			hornet.MessageIDs{milestoneMessageID},
			condition,
			nil,
			// called on missing parents
			// return error on missing parents
			nil,
			// called on solid entry points
			// Ignore solid entry points (snapshot milestone included)
			nil,
			false); err != nil {
			return err
		}

		// TODO:
		// Check correct ledger state
		// Rollback till genesis snapshot?
		// Or use temporary ledger to compare results?

		return nil
	}

	for msIndex := msIndexStart; msIndex <= msIndexEnd; msIndex++ {
		msgsCount := 0
		ts := time.Now()
		if err := checkMilestoneCone(
			ctx,
			tangleStore,
			milestoneManager,
			func(cachedMsg *storage.CachedMessage) {
				defer cachedMsg.Release(true)
				msgsCount++
			}, msIndex); err != nil {
			return err
		}
		println(fmt.Sprintf("successfully verified milestone cone %d, msgs: %d, total: %v", msIndex, msgsCount, time.Since(ts).Truncate(time.Millisecond)))
	}

	return nil
}

// storeMessage adds a new message to the storage,
// including all additional information like metadata, children,
// indexation, unreferenced messages and milestone entries.
func storeMessage(dbStorage *storage.Storage, milestoneManager *milestonemanager.MilestoneManager, message *storage.Message) *storage.CachedMessage {

	cachedMessage, isNew := dbStorage.StoreMessageIfAbsent(message) // msg +1

	if !isNew {
		// no need to process known messages
		return cachedMessage
	}

	for _, parent := range message.Parents() {
		dbStorage.StoreChild(parent, cachedMessage.Message().MessageID()).Release(true)
	}

	indexationPayload := storage.CheckIfIndexation(cachedMessage.Message())
	if indexationPayload != nil {
		// store indexation if the message contains an indexation payload
		dbStorage.StoreIndexation(indexationPayload.Index, cachedMessage.Message().MessageID()).Release(true)
	}

	if ms := milestoneManager.VerifyMilestone(message); ms != nil {
		cachedMilestone, newlyAdded := dbStorage.StoreMilestoneIfAbsent(milestone.Index(ms.Index), cachedMessage.Message().MessageID(), time.Unix(int64(ms.Timestamp), 0))
		if newlyAdded {
			// Force release to store milestones without caching
			cachedMilestone.Release(true) // milestone +-0
		}
	}

	return cachedMessage
}

func copyMilestoneCone(ctx context.Context,
	msIndex milestone.Index,
	milestoneMessageID hornet.MessageID,
	cachedMessageFuncSource storage.CachedMessageFunc,
	cachedMetadataFuncSource storage.CachedMessageMetadataFunc,
	solidEntryPointsContainFuncSource storage.SolidEntryPointsContainFunc,
	storeTarget *storage.Storage,
	milestoneManager *milestonemanager.MilestoneManager) error {

	// traversal stops if no more messages pass the given condition
	// Caution: condition func is not in DFS order
	condition := func(cachedMetadata *storage.CachedMetadata) (bool, error) { // meta +1
		defer cachedMetadata.Release(true) // meta -1

		// collect all msg that were referenced by that milestone
		referenced, at := cachedMetadata.Metadata().ReferencedWithIndex()

		if referenced {
			if at > msIndex {
				return false, fmt.Errorf("milestone cone inconsistent (msIndex: %d, referencedAt: %d)", msIndex, at)
			}

			if at < msIndex {
				// do not traverse messages that were referenced by an older milestonee
				return false, nil
			}
		}

		cachedMsg := cachedMessageFuncSource(cachedMetadata.Metadata().MessageID())
		if cachedMsg == nil {
			return false, fmt.Errorf("message not found: %s", cachedMetadata.Metadata().MessageID().ToHex())
		}
		defer cachedMsg.Release(true)

		// store the message in the target storage
		storeMessage(storeTarget, milestoneManager, cachedMsg.Message()).Release(true)
		println(fmt.Sprintf("STORED by HORNET: %s", cachedMetadata.Metadata().MessageID().ToHex()))
		return true, nil
	}

	// we don't need to call cleanup at the end, because we pass our own metadataMemcache.
	syncParentsTraverser := dag.NewParentTraverser(cachedMetadataFuncSource, solidEntryPointsContainFuncSource, nil)

	// traverse the milestone and collect all messages that were referenced by this milestone or newer
	if err := syncParentsTraverser.Traverse(
		ctx,
		hornet.MessageIDs{milestoneMessageID},
		condition,
		nil,
		// called on missing parents
		// return error on missing parents
		nil,
		// called on solid entry points
		// Ignore solid entry points (snapshot milestone included)
		nil,
		false); err != nil {
		return err
	}

	return nil
}

func copyAndVerifyMilestoneCone(
	ctx context.Context,
	msIndex milestone.Index,
	getMilestoneAndMessageID func(msIndex milestone.Index) (*storage.Message, hornet.MessageID, error),
	cachedMessageFuncSource storage.CachedMessageFunc,
	cachedMetadataFuncSource storage.CachedMessageMetadataFunc,
	solidEntryPointsContainFuncSource storage.SolidEntryPointsContainFunc,
	storeTarget *storage.Storage,
	milestoneManager *milestonemanager.MilestoneManager) error {

	if err := utils.ReturnErrIfCtxDone(ctx, common.ErrOperationAborted); err != nil {
		return err
	}

	msMsg, milestoneMessageID, err := getMilestoneAndMessageID(msIndex)
	if err != nil {
		return err
	}

	if ms := milestoneManager.VerifyMilestone(msMsg); ms == nil {
		return fmt.Errorf("source milestone not valid! %d", msIndex)
	}

	ts := time.Now()

	if err := copyMilestoneCone(
		context.Background(), // do not abort the copying of the messages itself
		msIndex,
		milestoneMessageID,
		cachedMessageFuncSource,
		cachedMetadataFuncSource,
		solidEntryPointsContainFuncSource,
		storeTarget,
		milestoneManager); err != nil {
		return err
	}

	timeCopyMilestoneCone := time.Now()

	var missingMsg hornet.MessageID
	confirmedMilestoneStats, _, err := whiteflag.ConfirmMilestone(
		storeTarget,
		nil,
		storeTarget.CachedMessageMetadataOrNil,
		func(messageID hornet.MessageID) *storage.CachedMessage {
			cachedMsg := storeTarget.CachedMessageOrNil(messageID)
			if cachedMsg == nil {
				println(fmt.Sprintf("LEL not found: %s", append(hornet.MessageID{common.StorePrefixMessages}, messageID...).ToHex()))
				/*
					cachedMsgSource := cachedMessageFuncSource(messageID)
					if cachedMsgSource == nil {
						println(fmt.Sprintf("LEL not found in source as well? WTF: %s", messageID.ToHex()))
						return nil
					}
					storeMessage(storeTarget, milestoneManager, cachedMsgSource.Message())
					cachedMsgSource.Release(true)

					cachedMsg = storeTarget.CachedMessageOrNil(messageID)
					if cachedMsg == nil {
						println(fmt.Sprintf("STILL not found: %s", messageID.ToHex()))
						missingMsg = messageID
					}
				*/
			}
			return cachedMsg
		},
		milestoneMessageID,
		nil,
		nil,
		nil,
		nil,
		nil)
	if err != nil {
		if missingMsg != nil {
			println("TRY TO SEARCH AGAIN")
			time.Sleep(1000 * time.Millisecond)

			cachedMsg := storeTarget.CachedMessageOrNil(missingMsg)
			if cachedMsg == nil {
				println(fmt.Sprintf("ROFL STILL NOT THERE: %s", missingMsg.ToHex()))
			} else {
				cachedMsg.Release(true)
			}
		}
		return err
	}
	timeConfirmMilestone := time.Now()
	println(fmt.Sprintf("\n\nconfirmed milestone %d, messages: %d, conflicts: %d, mutations: %d, data msgs: %d, durationCopyMilestoneCone: %v, durationConfirmMilestone: %v, total: %v", confirmedMilestoneStats.Index, confirmedMilestoneStats.MessagesReferenced, confirmedMilestoneStats.MessagesExcludedWithConflictingTransactions, confirmedMilestoneStats.MessagesIncludedWithTransactions, confirmedMilestoneStats.MessagesExcludedWithoutTransactions, timeCopyMilestoneCone.Sub(ts).Truncate(time.Millisecond), timeConfirmMilestone.Sub(timeCopyMilestoneCone).Truncate(time.Millisecond), timeConfirmMilestone.Sub(ts).Truncate(time.Millisecond)))
	return nil
}

func mergeDatabase(
	ctx context.Context,
	milestoneManager *milestonemanager.MilestoneManager,
	tangleStoreSource *storage.Storage,
	tangleStoreTarget *storage.Storage,
	client *iotago.NodeHTTPAPIClient) error {

	// what does that tool do
	// it should start with a genesis snapshot if target database is empty (no matter if milestone 1 or whatever)
	// snapshot must fit the applied milestones
	// all milestones and their changes are applied storing the cone (utxo changes etc) => but 1 by 1
	//
	// no matter what database format, we only need the tangle database
	// check network ID first
	// check existing milestones
	// if

	getMessageViaAPI := func(client *iotago.NodeHTTPAPIClient, messageID hornet.MessageID) (*storage.Message, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		ts := time.Now()
		msg, err := client.MessageJSONByMessageID(ctx, messageID.ToArray())
		if err != nil {
			return nil, err
		}
		println(fmt.Sprintf("message received: %s, took: %v", messageID.ToHex(), time.Since(ts).Truncate(time.Millisecond)))

		message, err := storage.NewMessage(msg, serializer.DeSeriModePerformValidation)
		if err != nil {
			return nil, errors.WithMessagef(restapi.ErrInvalidParameter, "invalid message, error: %s", err)
		}

		return message, nil
	}

	getMilestoneViaAPI := func(client *iotago.NodeHTTPAPIClient, getCachedMessageViaAPI storage.CachedMessageFunc, msIndex milestone.Index) (*storage.Message, hornet.MessageID, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		ms, err := client.MilestoneByIndex(ctx, uint32(msIndex))
		if err != nil {
			return nil, nil, err
		}

		messageID, err := hornet.MessageIDFromHex(ms.MessageID)
		if err != nil {
			return nil, nil, err
		}

		cachedMsg := getCachedMessageViaAPI(messageID) // message +1
		if cachedMsg == nil {
			return nil, nil, fmt.Errorf("message not found: %s", messageID.ToHex())
		}
		defer cachedMsg.Release(true) // message -1

		return cachedMsg.Message(), cachedMsg.Message().MessageID(), nil
	}

	msIndexStartSource, msIndexEndSource := getStorageMilestoneRange(tangleStoreSource)
	msIndexStartTarget, msIndexEndTarget := getStorageMilestoneRange(tangleStoreTarget)

	println(fmt.Sprintf("Milestone range source database: %d-%d", msIndexStartSource, msIndexEndSource))
	println(fmt.Sprintf("Milestone range target database: %d-%d", msIndexStartTarget, msIndexEndTarget))

	// case 1: milestones missing
	// case 2: no new milestones

	if msIndexEndSource <= msIndexEndTarget {
		return fmt.Errorf("%w (latest source index: %d, latest target index: %d)", ErrNoNewHistory, msIndexEndSource, msIndexEndTarget)
	}

	if msIndexStartTarget == msIndexEndTarget {
		// no genesis snapshot loader
		_, _, err := snapshot.LoadSnapshotFilesToStorage(context.Background(), tangleStoreTarget, "testdbs/genesis_snapshot.bin")
		if err != nil {
			return err
		}
	}

	msIndexStart := msIndexEndTarget + 1
	msIndexEnd := msIndexEndSource

	if msIndexStartSource > msIndexEndTarget+1 {
		if client == nil {
			return fmt.Errorf("history is missing (oldest source index: %d, latest target index: %d)", msIndexStartSource, msIndexEndTarget)
		}

		getCachedMessageViaAPI := func(messageID hornet.MessageID) *storage.CachedMessage { // message +1
			if !tangleStoreTarget.ContainsMessage(messageID) {
				msg, err := getMessageViaAPI(client, messageID)
				if err != nil {
					println(err)
					return nil
				}
				return storeMessage(tangleStoreTarget, milestoneManager, msg)
			}
			return tangleStoreTarget.CachedMessageOrNil(messageID)
		}

		getCachedMetadataViaAPI := func(messageID hornet.MessageID) *storage.CachedMetadata { // meta +1
			cachedMsg := getCachedMessageViaAPI(messageID) // message +1
			if cachedMsg == nil {
				return nil
			}
			defer cachedMsg.Release(true)     // message -1
			return cachedMsg.CachedMetadata() // meta +1
		}

		for msIndex := msIndexEndTarget + 1; msIndex < msIndexStartSource; msIndex++ {
			println(fmt.Sprintf("get milestone %d via API", msIndex))

			if err := copyAndVerifyMilestoneCone(
				ctx,
				msIndex,
				func(msIndex milestone.Index) (*storage.Message, hornet.MessageID, error) {
					return getMilestoneViaAPI(client, getCachedMessageViaAPI, msIndex)
				},
				getCachedMessageViaAPI,
				getCachedMetadataViaAPI,
				tangleStoreTarget.SolidEntryPointsContain,
				tangleStoreTarget,
				milestoneManager); err != nil {
				return err
			}
			msIndexStart = msIndex + 1
		}
	}

	for msIndex := msIndexStart; msIndex <= msIndexEnd; msIndex++ {
		println(fmt.Sprintf("get milestone %d via source database (%d-%d)", msIndex, msIndexStart, msIndexEnd))
		if err := copyAndVerifyMilestoneCone(
			ctx,
			msIndex,
			func(msIndex milestone.Index) (*storage.Message, hornet.MessageID, error) {
				milestoneMessageID, err := getMilestoneMessageIDFromStorage(tangleStoreSource, msIndex)
				if err != nil {
					return nil, nil, err
				}

				msMsg, err := getMilestoneMessageFromStorage(tangleStoreSource, milestoneMessageID)
				if err != nil {
					return nil, nil, err
				}

				return msMsg, milestoneMessageID, nil
			},
			tangleStoreSource.CachedMessageOrNil,
			tangleStoreSource.CachedMessageMetadataOrNil,
			tangleStoreSource.SolidEntryPointsContain,
			tangleStoreTarget,
			milestoneManager); err != nil {
			return err
		}
	}

	return nil
}

func databaseMerge(args []string) error {

	fs := flag.NewFlagSet("", flag.ContinueOnError)
	databaseVerifyTargetFlag := fs.Bool("verify", false, "verify existing target database")
	databasePathSourceFlag := fs.String(FlagToolDatabasePathSource, "", "the path to the source database")
	databasePathTargetFlag := fs.String(FlagToolDatabasePathTarget, "", "the path to the target database")
	databaseEngineSourceFlag := fs.String(FlagToolDatabaseEngineSource, string(DefaultValueDatabaseEngine), "the engine of the source database (values: pebble, rocksdb)")
	databaseEngineTargetFlag := fs.String(FlagToolDatabaseEngineTarget, string(DefaultValueDatabaseEngine), "the engine of the target database (values: pebble, rocksdb)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", ToolDatabaseMerge)
		fs.PrintDefaults()
		println(fmt.Sprintf("\nexample: %s --%s %s --%s %s",
			ToolDatabaseMerge,
			FlagToolDatabasePathSource,
			"mainnetdb/tangle",
			FlagToolDatabasePathTarget,
			"mergeddb/tangle",
		))
	}

	// check db network ID
	// check corrupted?
	// target is critical, source may be ok if everything is there
	// what about SEP?
	// what about state changes?
	// what about splitted DB or not?

	if err := parseFlagSet(fs, args); err != nil {
		return err
	}

	if len(*databasePathSourceFlag) == 0 {
		return fmt.Errorf("'%s' not specified", FlagToolDatabasePathSource)
	}
	if len(*databasePathTargetFlag) == 0 {
		return fmt.Errorf("'%s' not specified", FlagToolDatabasePathTarget)
	}
	if len(*databaseEngineSourceFlag) == 0 {
		return fmt.Errorf("'%s' not specified", FlagToolDatabaseEngineSource)
	}
	if len(*databaseEngineTargetFlag) == 0 {
		return fmt.Errorf("'%s' not specified", FlagToolDatabaseEngineTarget)
	}

	sourcePath := *databasePathSourceFlag
	if _, err := os.Stat(sourcePath); err != nil || os.IsNotExist(err) {
		return fmt.Errorf("'%s' (%s) does not exist", FlagToolDatabasePathSource, sourcePath)
	}

	targetPath := *databasePathTargetFlag

	sourceEngine, err := database.DatabaseEngine(*databaseEngineSourceFlag, database.EnginePebble, database.EngineRocksDB)
	if err != nil {
		return err
	}

	targetEngine, err := database.DatabaseEngine(*databaseEngineTargetFlag, database.EnginePebble, database.EngineRocksDB)
	if err != nil {
		return err
	}

	targetEngine = database.EngineMapDB

	sourceDatabaseExists, err := database.DatabaseExists(sourcePath)
	if err != nil {
		return err
	}

	if !sourceDatabaseExists {
		print(fmt.Sprintf("source database does not exist (%s)!\n", sourcePath))
		return nil
	}

	if err := databasecore.SplitIntoTangleAndUTXO(sourcePath, sourceEngine); err != nil {
		return err
	}

	storeTangleSource, err := database.StoreWithDefaultSettings(filepath.Join(sourcePath, TangleDatabaseDirectoryName), false, sourceEngine)
	if err != nil {
		return fmt.Errorf("source tangle database initialization failed: %w", err)
	}

	storeUTXOSource, err := database.StoreWithDefaultSettings(filepath.Join(sourcePath, UTXODatabaseDirectoryName), false, sourceEngine)
	if err != nil {
		return fmt.Errorf("source utxo database initialization failed: %w", err)
	}

	storeTangleTarget, err := database.StoreWithDefaultSettings(filepath.Join(targetPath, TangleDatabaseDirectoryName), true, targetEngine)
	if err != nil {
		return fmt.Errorf("target tangle database initialization failed: %w", err)
	}

	storeUTXOTarget, err := database.StoreWithDefaultSettings(filepath.Join(targetPath, UTXODatabaseDirectoryName), true, targetEngine)
	if err != nil {
		return fmt.Errorf("target utxo database initialization failed: %w", err)
	}

	tangleStoreSource, err := storage.New(storeTangleSource, storeUTXOSource, false)
	if err != nil {
		return err
	}

	tangleStoreTarget, err := storage.New(storeTangleTarget, storeUTXOTarget, true)
	if err != nil {
		return err
	}

	/*
		defer func() {
			println("Shutdown storages...")
			tangleStoreSource.ShutdownStorages()
			tangleStoreTarget.ShutdownStorages()

			println("FLush and close stores...")
			tangleStoreSource.FlushAndCloseStores()
			tangleStoreTarget.FlushAndCloseStores()
		}()
	*/

	nodeConfig, err := loadConfigFile("config.json")
	if err != nil {
		return err
	}

	var coordinatorPublicKeyRanges coordinator.PublicKeyRanges

	// load from config
	if err := nodeConfig.Unmarshal(CfgProtocolPublicKeyRanges, &coordinatorPublicKeyRanges); err != nil {
		return err
	}

	keyManager := keymanager.New()
	for _, keyRange := range coordinatorPublicKeyRanges {
		pubKey, err := utils.ParseEd25519PublicKeyFromString(keyRange.Key)
		if err != nil {
			return fmt.Errorf("can't load public key ranges: %w", err)
		}

		keyManager.AddKeyRange(pubKey, keyRange.StartIndex, keyRange.EndIndex)
	}

	milestoneManager := milestonemanager.New(nil, nil, keyManager, nodeConfig.Int(CfgProtocolMilestonePublicKeyCount))

	ctx, cancel := context.WithCancel(context.Background())

	gracefulStop := make(chan os.Signal, 1)
	signal.Notify(gracefulStop, syscall.SIGTERM)
	signal.Notify(gracefulStop, syscall.SIGINT)

	go func() {
		<-gracefulStop
		cancel()
	}()

	doWhatEver := func() error {

		if *databaseVerifyTargetFlag {
			println("verifying target database")
			if err := verifyDatabase(ctx, tangleStoreTarget, milestoneManager); err != nil {
				return err
			}
		}

		var client *iotago.NodeHTTPAPIClient
		if permascam != "" {
			requestURLHook := func(url string) string {
				return strings.Replace(url, "api/mainnet/api/v1/", "api/mainnet/", 1)
			}
			client = iotago.NewNodeHTTPAPIClient(permascam, iotago.WithNodeHTTPAPIClientRequestURLHook(requestURLHook))
		}

		corruptedTarget, err := tangleStoreTarget.AreDatabasesCorrupted()
		if err != nil {
			return err
		}

		if corruptedTarget {
			tangleStoreTarget.MarkDatabasesTainted()
			return errors.New("target database is corrupted")
		}

		taintedTarget, err := tangleStoreTarget.AreDatabasesTainted()
		if err != nil {
			return err
		}

		if taintedTarget {
			return errors.New("target database is tainted")
		}

		if err := tangleStoreTarget.MarkDatabasesCorrupted(); err != nil {
			return err
		}

		err = mergeDatabase(ctx, milestoneManager, tangleStoreSource, tangleStoreTarget, client)

		// ignore errors due to node shutdown
		if err != nil && !errors.Is(err, common.ErrOperationAborted) && !errors.Is(err, ErrNoNewHistory) {
			return err
		}

		msIndexStart, msIndexEnd := getStorageMilestoneRange(tangleStoreTarget)
		println(fmt.Sprintf("Milestone range target database: %d-%d", msIndexStart, msIndexEnd))

		// mark clean shutdown of the database
		if err := tangleStoreTarget.MarkDatabasesHealthy(); err != nil {
			return err
		}

		return nil
	}

	//startProfiling()

	err = doWhatEver()
	if err != nil {
		println(err.Error())
	}

	return err
}

func loadConfigFile(filePath string) (*configuration.Configuration, error) {
	config := configuration.New()

	if err := config.LoadFile(filePath); err != nil {
		return nil, fmt.Errorf("loading config file failed: %w", err)
	}

	return config, nil
}
