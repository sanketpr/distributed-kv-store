package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key       string
	OldValue  string
	NewValue  string
	RequestId int64
}

type PutAppendReply struct {
	Value        string
	ResponseCode int16
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value        string
	ResponseCode int16
}
