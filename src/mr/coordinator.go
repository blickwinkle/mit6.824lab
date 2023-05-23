package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	WaitAllocTask = iota
	PendingTask
	FinishedTask
)

type allocinfo struct {
	workerId  int
	taskType  int
	taskId    int
	allocTime int64
}

type Coordinator struct {
	// Your definitions here.
	nReduce                 int
	nMap                    int
	mapTaskStatus           []int
	reduceTaskStatus        []int
	mapTaskFile             []string
	mapTaskId               []allocinfo // Alloc map worker id
	reduceTaskId            []allocinfo // Alloc reduce worker id
	mapTaskFinishedCount    int
	reduceTaskFinishedCount int
	mapTaskFinished         bool
	reduceTaskFinished      bool
	mutex                   sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	//fmt.Printf("RequestTask: %v\n", args)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	reply.TaskType = WaitTask
	if c.mapTaskFinished && c.reduceTaskFinished {
		reply.TaskType = ExitTask
	} else if c.mapTaskFinished {
		for i := range c.reduceTaskStatus {
			if c.reduceTaskStatus[i] == WaitAllocTask {
				c.reduceTaskStatus[i] = PendingTask
				c.reduceTaskId[i] = allocinfo{args.WorkerId, ReduceTask, i, time.Now().Unix()}
				reply.TaskType = ReduceTask
				reply.TaskId = i
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				break
			}
		}
	} else {
		for i := range c.mapTaskStatus {
			if c.mapTaskStatus[i] == WaitAllocTask {
				c.mapTaskStatus[i] = PendingTask
				c.mapTaskId[i] = allocinfo{args.WorkerId, MapTask, i, time.Now().Unix()}
				reply.TaskType = MapTask
				reply.TaskId = i
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				reply.FileName = c.mapTaskFile[i]
				break
			}
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	ret := c.reduceTaskFinished

	// Your code here.

	return ret
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.TaskType == MapTask {
		if c.mapTaskStatus[args.TaskId] != FinishedTask {
			c.mapTaskFinishedCount++
			if c.mapTaskFinishedCount == c.nMap {
				c.mapTaskFinished = true
			}
		}
		c.mapTaskStatus[args.TaskId] = FinishedTask
	} else {
		if c.reduceTaskStatus[args.TaskId] != FinishedTask {
			c.reduceTaskFinishedCount++
			if c.reduceTaskFinishedCount == c.nReduce {
				c.reduceTaskFinished = true
			}
		}
		c.reduceTaskStatus[args.TaskId] = FinishedTask
	}
	return nil
}

func (c *Coordinator) checkTimeOut() {
	for {
		time.Sleep(time.Second)
		c.mutex.Lock()
		if !c.mapTaskFinished {
			for i := range c.mapTaskId {
				if c.mapTaskStatus[i] == PendingTask && time.Now().Unix()-c.mapTaskId[i].allocTime > 10 {
					c.mapTaskStatus[i] = WaitAllocTask
				}
			}
		} else if !c.reduceTaskFinished {
			for i := range c.reduceTaskId {
				if c.reduceTaskStatus[i] == PendingTask && time.Now().Unix()-c.reduceTaskId[i].allocTime > 10 {
					c.reduceTaskStatus[i] = WaitAllocTask
				}
			}
		} else {
			c.mutex.Unlock()
			return // All task finished
		}
		c.mutex.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTaskFile: files, nReduce: nReduce, nMap: len(files),
		mapTaskStatus: make([]int, len(files)), reduceTaskStatus: make([]int, nReduce),
		mapTaskId: make([]allocinfo, len(files)), reduceTaskId: make([]allocinfo, nReduce),
		mapTaskFinished:         false,
		reduceTaskFinished:      false,
		mapTaskFinishedCount:    0,
		reduceTaskFinishedCount: 0,
	}
	for i := range c.mapTaskStatus {
		c.mapTaskStatus[i] = WaitAllocTask
	}
	for i := range c.reduceTaskStatus {
		c.reduceTaskStatus[i] = WaitAllocTask
	}
	// Your code here.
	go c.checkTimeOut()

	c.server()
	return &c
}
