package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	processedRequestIds sync.Map
	store               sync.Map
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	val, ok := kv.store.Load(args.Key)
	if ok {
		reply.Value = val.(string)
		reply.ResponseCode = 200
	} else {
		reply.ResponseCode = 404
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	val, isKeyPresent := kv.store.Load(args.Key)
	if isKeyPresent {
		isUpdateSuccess := kv.store.CompareAndSwap(args.Key, args.OldValue, args.NewValue)
		if isUpdateSuccess {
			reply.ResponseCode = 200
			reply.Value = args.OldValue
		} else {
			reply.ResponseCode = 409
			reply.Value = val.(string)
		}
	} else {
		kv.store.Store(args.Key, args.NewValue)
		reply.ResponseCode = 200
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	dupOldValue, isDup := kv.processedRequestIds.Load(args.RequestId)
	if isDup {
		reply.ResponseCode = 200
		reply.Value = dupOldValue.(string)
		return
	}

	val, ok := kv.store.Load(args.Key)

	if ok {
		newAppendValue := val.(string) + args.NewValue
		isUpdateSuccess := kv.store.CompareAndSwap(args.Key, args.OldValue, newAppendValue)
		reply.Value = args.OldValue
		if isUpdateSuccess {
			kv.processedRequestIds.Store(args.RequestId, args.OldValue)
			reply.ResponseCode = 200
		} else {
			reply.ResponseCode = 409
		}
	} else {
		reply.ResponseCode = 404
		reply.Value = ""
		kv.processedRequestIds.Store(args.RequestId, "")
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	return kv
}
