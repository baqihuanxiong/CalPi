package main

import (
	"CalPi/rcmsg"
	"context"
	"encoding/json"
	"flag"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/glog"
	uuid "github.com/satori/go.uuid"
	"google.golang.org/grpc"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"runtime"
	"time"
)

type Config struct {
	HostName   string `json:"HostName"`
	GrpcAddr   string `json:"grpcAddr"`
	RedisAddr  string `json:"redisAddr"`
	GOMAXPROCS int    `json:"GOMAXPROCS"`
}

type Result struct {
	Id      int64
	Darts   int64
	Hits    int64
	Consume int64
}

var (
	hostName    string
	config      Config
	taskInChan  chan *rcmsg.CalRequest
	taskOutChan chan *Result
)

type RPCServer struct{}

func (s *RPCServer) MonteCarlo(ctx context.Context, in *rcmsg.CalRequest) (*rcmsg.CalReply, error) {
	taskInChan <- in
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
	if config.GOMAXPROCS > 0 {
		runtime.GOMAXPROCS(config.GOMAXPROCS)
	}

	c, err := redis.Dial("tcp", config.RedisAddr)
	if err != nil {
		panic(err)
	}

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

	for {
		select {
		case req := <-taskInChan:
			go CalPi(req.Id, req.Darts)
			glog.V(4).Infoln("Started task %d", req.Id)
		case result := <-taskOutChan:
			err := RedisSetPiResult(c, result)
			if err != nil {
				glog.Errorln("Write redis error:", err)
			}
		}
	}
}

func CalPi(id, darts int64) {
	start := time.Now()
	result := &Result{Id: id, Darts: darts, Hits: 0}
	var i int64
	for i = 0; i < darts; i++ {
		x, y := rand.Float64(), rand.Float64()
		if math.Sqrt(x*x+y*y) < 1 {
			result.Hits++
		}
	}
	result.Consume = time.Since(start).Nanoseconds()
	taskOutChan <- result
}

func RedisSetPiResult(redisConn redis.Conn, result *Result) error {
	baseKeyName := "PiTemp:" + hostName
	_ = redisConn.Send("MULTI")
	_ = redisConn.Send("MSET", baseKeyName+":ID", result.Id,
		baseKeyName+":Darts", result.Darts, baseKeyName+":Consume", result.Consume)
	_ = redisConn.Send("INCRBY", baseKeyName+":Hits", result.Hits)
	_ = redisConn.Send("EXEC")
	_, err := redisConn.Do("")
	if err != nil {
		return err
	}
	return nil
}
