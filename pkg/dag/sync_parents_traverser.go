package dag

import (
	"container/list"
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/gohornet/hornet/pkg/common"
	"github.com/gohornet/hornet/pkg/model/hornet"
	"github.com/gohornet/hornet/pkg/model/storage"
	"github.com/gohornet/hornet/pkg/utils"
)

// the default options applied to the SyncParentTraverser.
var defaultSyncParentTraverserOptions = []SyncParentTraverserOption{
	WithSyncParentTraverserParallelism(runtime.NumCPU()),
}

// SyncParentTraverserOption is a function setting a SyncParentTraverserOption option.
type SyncParentTraverserOption func(opts *SyncParentTraverserOptions)

// SyncParentTraverserOptions define options for the SyncParentTraverser.
type SyncParentTraverserOptions struct {
	// The function to modify the request url.
	parallelism int
}

// applies the given SyncParentTraverserOption.
func (so *SyncParentTraverserOptions) apply(opts ...SyncParentTraverserOption) {
	for _, opt := range opts {
		opt(so)
	}
}

// WithSyncParentTraverserParallelism sets the used parallelism.
func WithSyncParentTraverserParallelism(parallelism int) SyncParentTraverserOption {
	return func(opts *SyncParentTraverserOptions) {
		opts.parallelism = parallelism
	}
}

type SyncParentTraverser struct {
	cachedMetadataFunc          storage.CachedMessageMetadataFunc
	solidEntryPointsContainFunc storage.SolidEntryPointsContainFunc
	cleanupFunc                 storage.CleanupFunc

	// holds the SyncParentTraverser options.
	opts *SyncParentTraverserOptions

	// stack holding the ordered msg to process
	stack *list.List

	// processed map with already processed messages
	processed map[string]struct{}

	// checked map with result of traverse condition
	checked map[string]bool

	ctx                      context.Context
	condition                Predicate
	consumer                 Consumer
	onMissingParent          OnMissingParent
	onSolidEntryPoint        OnSolidEntryPoint
	traverseSolidEntryPoints bool

	traverserLock sync.Mutex
}

// NewSyncParentTraverser create a new traverser to traverse the parents (past cone)
func NewSyncParentTraverser(
	cachedMetadataFunc storage.CachedMessageMetadataFunc,
	solidEntryPointsContainFunc storage.SolidEntryPointsContainFunc,
	cleanupFunc storage.CleanupFunc,
	opts ...SyncParentTraverserOption) *SyncParentTraverser {

	options := &SyncParentTraverserOptions{}
	options.apply(defaultSyncParentTraverserOptions...)
	options.apply(opts...)

	t := &SyncParentTraverser{
		cachedMetadataFunc:          cachedMetadataFunc,
		solidEntryPointsContainFunc: solidEntryPointsContainFunc,
		cleanupFunc:                 cleanupFunc,
		stack:                       list.New(),
		processed:                   make(map[string]struct{}),
		checked:                     make(map[string]bool),
		opts:                        options,
	}

	return t
}

func (st *SyncParentTraverser) reset() {

	st.processed = make(map[string]struct{})
	st.checked = make(map[string]bool)
	st.stack = list.New()
}

// Cleanup releases all the cached objects that have been traversed.
// This MUST be called by the user at the end.
func (st *SyncParentTraverser) Cleanup(forceRelease bool) {
	if st.cleanupFunc != nil {
		st.cleanupFunc(forceRelease)
	}
}

// Traverse starts to traverse the parents (past cone) in the given order until
// the traversal stops due to no more messages passing the given condition.
// It is a DFS of the paths of the parents one after another.
// Caution: condition func is not in DFS order
func (st *SyncParentTraverser) Traverse(ctx context.Context, parents hornet.MessageIDs, condition Predicate, consumer Consumer, onMissingParent OnMissingParent, onSolidEntryPoint OnSolidEntryPoint, traverseSolidEntryPoints bool) error {

	// make sure only one traversal is running
	st.traverserLock.Lock()

	// release lock so the traverser can be reused
	defer st.traverserLock.Unlock()

	st.ctx = ctx
	st.condition = condition
	st.consumer = consumer
	st.onMissingParent = onMissingParent
	st.onSolidEntryPoint = onSolidEntryPoint
	st.traverseSolidEntryPoints = traverseSolidEntryPoints

	// Prepare for a new traversal
	st.reset()

	// we feed the stack with the parents one after another,
	// to make sure that we examine all paths.
	// however, we only need to do it if the parent wasn't processed yet.
	// the referenced parent message could for example already be processed
	// if it is directly/indirectly approved by former parents.
	for _, parent := range parents {
		st.stack.PushFront(parent)

		for i := 0; i < st.opts.parallelism; i++ {
			for st.stack.Len() > 0 {
				if err := st.processStackParents(); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// processStackParents checks if the current element in the stack must be processed or traversed.
// the paths of the parents are traversed one after another.
// Caution: condition func is not in DFS order
func (st *SyncParentTraverser) processStackParents() error {

	if err := utils.ReturnErrIfCtxDone(st.ctx, common.ErrOperationAborted); err != nil {
		return err
	}

	// load candidate msg
	ele := st.stack.Front()
	currentMessageID := ele.Value.(hornet.MessageID)
	currentMessageIDMapKey := currentMessageID.ToMapKey()

	if _, wasProcessed := st.processed[currentMessageIDMapKey]; wasProcessed {
		// message was already processed
		// remove the message from the stack
		st.stack.Remove(ele)
		return nil
	}

	// check if the message is a solid entry point
	if st.solidEntryPointsContainFunc(currentMessageID) {
		if st.onSolidEntryPoint != nil {
			st.onSolidEntryPoint(currentMessageID)
		}

		if !st.traverseSolidEntryPoints {
			// remove the message from the stack, the parents are not traversed
			st.processed[currentMessageIDMapKey] = struct{}{}
			delete(st.checked, currentMessageIDMapKey)
			st.stack.Remove(ele)
			return nil
		}
	}

	cachedMsgMeta := st.cachedMetadataFunc(currentMessageID) // meta +1
	if cachedMsgMeta == nil {
		// remove the message from the stack, the parents are not traversed
		st.processed[currentMessageIDMapKey] = struct{}{}
		delete(st.checked, currentMessageIDMapKey)
		st.stack.Remove(ele)

		if st.onMissingParent == nil {
			// stop processing the stack with an error
			return fmt.Errorf("%w: message %s", common.ErrMessageNotFound, currentMessageID.ToHex())
		}

		// stop processing the stack if the caller returns an error
		return st.onMissingParent(currentMessageID)
	}
	defer cachedMsgMeta.Release(true) // meta -1

	traverse, checkedBefore := st.checked[currentMessageIDMapKey]
	if !checkedBefore {
		var err error

		// check condition to decide if msg should be consumed and traversed
		traverse, err = st.condition(cachedMsgMeta.Retain()) // meta + 1
		if err != nil {
			// there was an error, stop processing the stack
			return err
		}

		// mark the message as checked and remember the result of the traverse condition
		st.checked[currentMessageIDMapKey] = traverse
	}

	if !traverse {
		// remove the message from the stack, the parents are not traversed
		// parent will not get consumed
		st.processed[currentMessageIDMapKey] = struct{}{}
		delete(st.checked, currentMessageIDMapKey)
		st.stack.Remove(ele)
		return nil
	}

	for _, parentMessageID := range cachedMsgMeta.Metadata().Parents() {
		if _, parentProcessed := st.processed[parentMessageID.ToMapKey()]; !parentProcessed {
			// parent was not processed yet
			// traverse this message
			st.stack.PushFront(parentMessageID)
			return nil
		}
	}

	// remove the message from the stack
	st.processed[currentMessageIDMapKey] = struct{}{}
	delete(st.checked, currentMessageIDMapKey)
	st.stack.Remove(ele)

	if st.consumer != nil {
		// consume the message
		if err := st.consumer(cachedMsgMeta.Retain()); err != nil { // meta +1
			// there was an error, stop processing the stack
			return err
		}
	}

	return nil
}
