from multiprocessing import Process, Queue, set_start_method
import threading
import time
import uuid
import heapq

TX_BURST = {
    "BALANCE": 1,
    "DEPOSIT": 2,
    "WITHDRAW": 2,
    "TRANSFER": 4,
    "HEAVY_CALC": 8,
    "INITIAL_BLOCK": 2
}

MAX_WORKERS = 1
AGING_INTERVAL = 1.5
STARVATION_THRESHOLD = 4


class ThreadControlBlock:
    def __init__(self, tid, tx_type):
        self.tid = tid
        self.tx_type = tx_type
        self.burst_time = TX_BURST.get(tx_type, 2)
        self.priority = self.burst_time
        self.arrival_time = time.time()
        self.state = "NEW"

    def __lt__(self, other):
        if self.priority == other.priority:
            return self.arrival_time < other.arrival_time
        return self.priority < other.priority


class BranchProcess(Process):
    def __init__(self, name, request_queue, response_queue):
        super().__init__()
        self.name = name
        self.request_queue = request_queue
        self.response_queue = response_queue

    def log(self, msg):
        print(f"[{self.name}] {msg}")

    def aging_worker(self):
        while True:
            with self.condition:
                self.condition.wait(timeout=AGING_INTERVAL)

                if not self.running and not self.ready_queue:
                    break

                now = time.time()
                temp = []
                changed = False

                while self.ready_queue:
                    tcb = heapq.heappop(self.ready_queue)
                    wait_time = now - tcb.arrival_time

                    if wait_time > STARVATION_THRESHOLD and tcb.priority > 1:
                        self.log(f"[AGING-MONITOR] Starvation Detected: {tcb.tid[-4:]} ({int(wait_time)}s)")
                        tcb.priority = max(1, tcb.priority - 2)
                        tcb.arrival_time = now
                        self.log(f"[AGING-APPLIED] {tcb.tid[-4:]} New Priority: {tcb.priority}")
                        changed = True

                    temp.append(tcb)

                for t in temp:
                    heapq.heappush(self.ready_queue, t)

                if changed:
                    self.condition.notify_all()

    def worker_loop(self, worker_id):
        self.log(f"Worker-{worker_id} Ready.")
        while True:
            with self.condition:
                while self.running and not self.ready_queue:
                    self.condition.wait()

                if not self.running and not self.ready_queue:
                    break

                tcb = heapq.heappop(self.ready_queue)
                tcb.state = "RUNNING"

            self.execute_transaction(tcb, worker_id)

    def execute_transaction(self, tcb, worker_id):
        self.log(f"[RUNNING] Worker-{worker_id} started {tcb.tid[-4:]} ({tcb.tx_type}, Burst={tcb.burst_time}s)")
        time.sleep(tcb.burst_time)
        tcb.state = "TERMINATED"
        self.log(f"[DONE] Worker-{worker_id} finished {tcb.tid[-4:]}")
        self.response_queue.put({"id": tcb.tid, "status": "DONE"})

    def run(self):
        self.ready_queue = []
        self.condition = threading.Condition()
        self.running = True

        self.log(f"Process Started. Policy: SJF + Aging | Workers: {MAX_WORKERS}")

        workers = []
        for i in range(MAX_WORKERS):
            t = threading.Thread(target=self.worker_loop, args=(i + 1,))
            t.start()
            workers.append(t)

        aging_thread = threading.Thread(target=self.aging_worker)
        aging_thread.start()

        while True:
            req = self.request_queue.get()
            if req.get("type") == "TERMINATE":
                break

            tcb = ThreadControlBlock(req["request_id"], req["type"])

            with self.condition:
                tcb.state = "READY"
                heapq.heappush(self.ready_queue, tcb)
                self.log(f"[NEW REQUEST] {tcb.tx_type} (ID: {tcb.tid[-4:]}, Burst: {tcb.burst_time})")
                self.condition.notify()

        with self.condition:
            self.running = False
            self.condition.notify_all()

        for w in workers:
            w.join()

        aging_thread.join()
        self.log("Process Terminated.")


def main():
    set_start_method("spawn", force=True)

    req_q = Queue()
    res_q = Queue()

    branch = BranchProcess("Central_Branch", req_q, res_q)
    branch.start()

    req_q.put({"request_id": str(uuid.uuid4()), "type": "INITIAL_BLOCK"})
    time.sleep(0.1)

    heavy_id = str(uuid.uuid4())
    req_q.put({"request_id": heavy_id, "type": "HEAVY_CALC"})
    time.sleep(0.1)

    for _ in range(7):
        req_q.put({"request_id": str(uuid.uuid4()), "type": "BALANCE"})
        time.sleep(0.2)

    time.sleep(15)

    req_q.put({"type": "TERMINATE"})
    branch.join()

    while not res_q.empty():
        print("Response:", res_q.get())

    print("\n[SYSTEM] phase 3 done")


if __name__ == "__main__":
    main()