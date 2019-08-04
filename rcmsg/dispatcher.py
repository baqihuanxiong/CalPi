import grpc
import redis
import time
import argparse
import logging
import numpy as np
import pandas as pd
import message_pb2
import message_pb2_grpc


logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)

parser = argparse.ArgumentParser(description='CalPi task dispatcher.')
parser.add_argument("-g", "--grpc-addr", type=str, default="127.0.0.1:50051", help="gRPC server address")
parser.add_argument("-r", "--redis-addr", type=str, default="127.0.0.1:6379", help="redis server address")
parser.add_argument("-d", "--redis-db", type=int, default=0, help="redis db")
parser.add_argument("-D", "--darts", type=int, default=int(1e8), help="Darts per task")
parser.add_argument("-i", "--interval", type=int, default=1, help="interval seconds")
parser.add_argument("-n", "--num", type=int, default=0, help="number of tasks, infinite if zero")
parser.add_argument("-t", "--timeout", type=int, default=0, help="stop send task when timeout(second) reach, infinite if zero")
args = parser.parse_args()


class Dispatcher:
    def __init__(self, grpc_addr, redis_addr):
        self._chan = grpc.insecure_channel(grpc_addr)
        self._stub = message_pb2_grpc.CalculatorStub(self._chan)
        redis_host = redis_addr.split(":")[0]
        redis_port = int(redis_addr.split(":")[1])
        self._redis = redis.Redis(host=redis_host, port=redis_port, db=0)
    
    def send(self, darts):
        tid = self._redis.get("PiTemp:tid")
        if tid is None:
            tid = 0
        req = message_pb2.CalRequest(id=int(tid), darts=darts, timestamp=time.time_ns())
        res = self._stub.MonteCarlo(req)
        if res.id == -1:
            logging.warning("Task pending because server is busy, server time {}".format(res.timestamp))
            return
        self._redis.incrby("PiTemp:tid", 1)
        logging.debug("Send task {} success, server time {}".format(res.id, res.timestamp))

    def query_pi(self):
        cols = ["Consume", "Darts", "Hits", "Timestamp"]
        result = pd.DataFrame(columns=cols, dtype=np.int64)
        nodes = [node.decode() for node in self._redis.hgetall("PiTemp:WorkNodes")]
        tmp = self._redis.mget(["PiTemp:"+node+":"+col for col in cols for node in nodes])
        for i, col in enumerate(cols):
            result[col] = [np.nan] * len(nodes)
            for j, v in enumerate(tmp[i*len(nodes):(i+1)*len(nodes)]):
                if v is not None:
                    result[col][j] = int(v.decode())
        result.index = nodes
        return result


def run():
    dispatcher = Dispatcher(args.grpc_addr, args.redis_addr)
    logging.info("Dispatcher start working...")
    count = 0
    start_time = time.time()
    while args.num == 0 or (args.num > 0 and count < args.num):
        if args.timeout > 0 and time.time() - start_time > args.timeout:
            break
        dispatcher.send(args.darts)
        time.sleep(args.interval)
        result = dispatcher.query_pi()
        pi = 4*(sum(result["Hits"])/sum(result["Darts"])) if sum(result["Darts"]) > 0 else np.inf
        logging.info("Working node: {}, work time: {} s, efficiency: {} darts/s, timestamp: {}, current pi is {}".format(
            list(result.index), list(result["Consume"]), list(result["Darts"]/result["Consume"]), list(result["Timestamp"]), pi))
        count += 1
    logging.info("Done.")
    

if __name__ == '__main__':
    run()
    
    