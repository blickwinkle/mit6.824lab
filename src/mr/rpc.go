package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

const (
	MapTask    = 0
	ReduceTask = 1
	WaitTask   = 2
	ExitTask   = 3
)

type RequestTaskArgs struct {
	WorkerId int
}

type RequestTaskReply struct {
	TaskType int
	TaskId   int
	NReduce  int
	NMap     int
	FileName string
}

type ReportTaskArgs struct {
	TaskType int
	TaskId   int
	WorkerId int
}

type ReportTaskReply struct {
	Success bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
