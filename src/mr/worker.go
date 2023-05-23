package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const (
	Running = iota
	Idle
	Finished
)

var WorkerId int
var WorkerStatus = Idle

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	WorkerId = os.Getpid()
	fmt.Printf("Worker %d started\n", WorkerId)
	for WorkerStatus != Finished {
		task := CallRequestTask()
		HandleTask(task.TaskType, task.TaskId, task.NReduce, task.NMap, task.FileName, mapf, reducef)
	}
	fmt.Printf("Worker %d finished\n", WorkerId)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func CallRequestTask() *RequestTaskReply {
	args := RequestTaskArgs{WorkerId}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		fmt.Printf("reply %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return &reply
}

func CallReportTask(taskType int, taskId int) *ReportTaskReply {
	args := ReportTaskArgs{taskType, taskId, WorkerId}
	reply := ReportTaskReply{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if ok {
		fmt.Printf("reply %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return &reply
}

func HandleTask(taskType int, taskId int, nReduce int, nMap int, fileName string,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	switch taskType {
	case MapTask:
		HandleMapTask(taskId, nReduce, fileName, mapf)
	case ReduceTask:
		HandleReduceTask(taskId, nMap, reducef)
	case WaitTask:
		HandleWaitTask()
	case ExitTask:
		HandleExitTask()
	}
}

func HandleMapTask(taskId, nReduce int, fileName string, mapf func(string, string) []KeyValue) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))

	for i := 0; i < nReduce; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", taskId, i)
		file, err := os.Create(fileName)
		if err != nil {
			log.Fatalf("cannot create %v", fileName)
		}
		enc := json.NewEncoder(file)
		for _, kv := range kva {
			if ihash(kv.Key)%nReduce == i {
				// fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
				enc.Encode(&kv)
			}
		}
		file.Close()
	}
	CallReportTask(MapTask, taskId)
}

func HandleReduceTask(taskId, nMap int, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for i := 0; i < nMap; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, taskId)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))

	fileName := fmt.Sprintf("mr-out-%d", taskId)
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("cannot create %v", fileName)
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)

		i = j
	}
	file.Close()
	CallReportTask(ReduceTask, taskId)
}

func HandleWaitTask() {
	WorkerStatus = Idle
	time.Sleep(1 * time.Second)
}

func HandleExitTask() {
	WorkerStatus = Finished
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
