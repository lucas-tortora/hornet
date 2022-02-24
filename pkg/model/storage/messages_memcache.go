package storage

import (
	"github.com/gohornet/hornet/pkg/model/hornet"
)

type SolidEntryPointsContainFunc func(messageID hornet.MessageID) bool

type ChildrenMessageIDsFunc func(messageID hornet.MessageID, iteratorOptions ...IteratorOption) hornet.MessageIDs

type CleanupFunc func(forceRelease bool)

type CachedMessageFunc func(messageID hornet.MessageID) *CachedMessage

type MessagesMemcache struct {
	cachedMessageFunc CachedMessageFunc
	cachedMsgs        map[string]*CachedMessage
}

// NewMessagesMemcache creates a new MessagesMemcache instance.
func NewMessagesMemcache(cachedMessageFunc CachedMessageFunc) *MessagesMemcache {
	return &MessagesMemcache{
		cachedMessageFunc: cachedMessageFunc,
		cachedMsgs:        make(map[string]*CachedMessage),
	}
}

// Cleanup releases all the cached objects that have been used.
// This MUST be called by the user at the end.
func (c *MessagesMemcache) Cleanup(forceRelease bool) {

	// release all msgs at the end
	for _, cachedMsg := range c.cachedMsgs {
		cachedMsg.Release(forceRelease) // meta -1
	}
	c.cachedMsgs = make(map[string]*CachedMessage)
}

// CachedMessageOrNil returns a cached message object.
// msg +1
func (c *MessagesMemcache) CachedMessageOrNil(messageID hornet.MessageID) *CachedMessage {
	messageIDMapKey := messageID.ToMapKey()

	// load up msg
	cachedMsg, exists := c.cachedMsgs[messageIDMapKey]
	if !exists {
		cachedMsg = c.cachedMessageFunc(messageID) // msg +1 (this is the one that gets cleared by "Cleanup")
		if cachedMsg == nil {
			return nil
		}

		// add the cachedObject to the map, it will be released by calling "Cleanup" at the end
		c.cachedMsgs[messageIDMapKey] = cachedMsg
	}

	return cachedMsg.Retain() // msg +1
}
