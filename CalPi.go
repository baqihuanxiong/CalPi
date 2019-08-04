package main

import (
	"CalPi/rcmsg"
	"context"
	"encoding/json"
	"flag"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/glog"
	uuid "github.com/satori/go.uuid"
	"github.com/valyala/fastrand"
	"google.golang.org/grpc"
	"io/ioutil"
	"math"
	"net"
	"os"
	"runtime"
	"time"
)

type Config struct {
	HostName       string `json:"hostName"`
	GrpcAddr       string `json:"grpcAddr"`
	RedisAddr      string `json:"redisAddr"`
	GOMAXPROCS     int    `json:"GOMAXPROCS"`
	TASKCHANLENGTH int    `json:"TASKCHANLENGTH"`
}

type Result struct {
	Id        int64
	Darts     int64
	Hits      int64
	Consume   int64
	Timestamp int64
}

var (
	hostName    string
	config      Config
	cpuNum      int
	taskRunChan chan *rcmsg.CalRequest
	taskInChan  chan *rcmsg.CalRequest
	taskOutChan chan *Result
	runningProc chan int
)

type RPCServer struct{}

func (s *RPCServer) MonteCarlo(ctx context.Context, in *rcmsg.CalRequest) (*rcmsg.CalReply, error) {
	if len(taskInChan) == config.TASKCHANLENGTH {
		glog.V(4).Infoln("gRPC server reject MonteCarlo request, running process:", len(runningProc))
		return &rcmsg.CalReply{Id: -1, Timestamp: time.Now().UnixNano()}, nil
	}
	taskInChan <- in
	glog.V(4).Infoln("gRPC server get MonteCarlo request:", in)
	return &rcmsg.CalReply{Id: in.Id, Timestamp: time.Now().UnixNano()}, nil
}

func init() {
	confFile := flag.String("c", "./config.json", "Config file for CalPi")
	cfg, err := ioutil.ReadFile(*confFile)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(cfg, &config)
	if err != nil {
		panic(err)
	}
	flag.Parse()
}

func main() {
	if config.HostName == "" {
		uid, _ := uuid.NewV4()
		hostName = uid.String()
	} else {
		hostName = config.HostName
	}
	cpuNum = runtime.NumCPU()
	if config.GOMAXPROCS > 0 {
		cpuNum = config.GOMAXPROCS
	}

	taskRunChan = make(chan *rcmsg.CalRequest, 1)
	taskInChan = make(chan *rcmsg.CalRequest, config.TASKCHANLENGTH)
	taskOutChan = make(chan *Result, config.TASKCHANLENGTH)
	runningProc = make(chan int, cpuNum)

	c, err := redis.Dial("tcp", config.RedisAddr)
	if err != nil {
		panic(err)
	}
	err = RedisSetWorkNode(c)
	if err != nil {
		panic(err)
	}
	glog.V(3).Infoln("Work node", hostName, "established, detect", runtime.NumCPU(), "cpus")

	lis, err := net.Listen("tcp", config.GrpcAddr)
	if err != nil {
		panic(err)
	}
	go func() {
		s := grpc.NewServer()
		rcmsg.RegisterCalculatorServer(s, &RPCServer{})
		err := s.Serve(lis)
		if err != nil {
			panic(err)
		}
	}()
	glog.V(3).Infoln("gRPC server listening on", config.GrpcAddr)

	go func() {
		for {
			runningProc <- 1
			t := <-taskInChan
			taskRunChan <- t
		}
	}()

	for {
		select {
		case req := <-taskRunChan:
			go CalPi(req.Id, req.Darts)
			glog.V(4).Infoln("Started task", req.Id)
		case result := <-taskOutChan:
			err := RedisSetPiResult(c, result)
			if err != nil {
				glog.Errorln("Write redis error:", err)
			}
			glog.V(4).Infoln("Task", result.Id, "finished")
		}
	}
}

func CalPi(id, darts int64) {
	start := time.Now()
	result := &Result{Id: id, Darts: darts, Hits: 0}
	var i int64
	for i = 0; i < darts; i++ {
		x, y := float64(fastrand.Uint32n(1e9))/1e9, float64(fastrand.Uint32n(1e9))/1e9
		if math.Sqrt(x*x+y*y) < 1 {
			result.Hits++
		}
	}
	result.Consume = time.Since(start).Nanoseconds()
	result.Timestamp = time.Now().UnixNano()
	taskOutChan <- result
	<-runningProc
}

func RedisSetWorkNode(redisConn redis.Conn) error {
	osHostName, _ := os.Hostname()
	_, err := redisConn.Do("HMSET", "PiTemp:WorkNodes", hostName, osHostName)
	if err != nil {
		return err
	}
	return nil
}

func RedisSetPiResult(redisConn redis.Conn, result *Result) error {
	baseKeyName := "PiTemp:" + hostName
	_ = redisConn.Send("MULTI")
	_ = redisConn.Send("MSET", baseKeyName+":Timestamp", result.Timestamp,
		baseKeyName+":RunningProc", len(runningProc))
	_ = redisConn.Send("INCRBY", baseKeyName+":Consume", result.Consume)
	_ = redisConn.Send("INCRBY", baseKeyName+":Darts", result.Darts)
	_ = redisConn.Send("INCRBY", baseKeyName+":Hits", result.Hits)
	_ = redisConn.Send("EXEC")
	_, err := redisConn.Do("")
	if err != nil {
		return err
	}
	return nil
}
