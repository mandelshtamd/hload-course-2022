package dqueue

import "errors"

var QueueIsEmptyErr = errors.New("Queue is empty")
var InternalErrMsg = "Internal Error"
var InternalErr = errors.New(InternalErrMsg)
var TooMuchShardsErr = errors.New("Too much shards")
