from multiprocessing import Process, Queue, set_start_method
import threading
import time
import uuid

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
        self.total_burst = TX_BURST.get(tx_type, 2)
        self.remaining = self.total_burst
        self.level = 0
        self.arrival_time = time.time()
        self.state = "NEW"


class BranchProcess(Process):
    def __init__(self, name, request_queue, response_queue):
        super().__init__()
        self.name = name
        self.request_queue = request_queue
        self.response_queue = response_queue

    def log(self, msg):
        print(f"[{self.name}] {msg}")

    def schedule_next(self):
        for q in self.mlfq:
            if q:
                return q.pop(0)
        return None

    def aging_worker(self):
        while True:
            time.sleep(AGING_INTERVAL)
            with self.condition:
                if not self.running and not any(self.mlfq):
                    break
                now = time.time()
                for lvl in range(1, 3):
                    temp = []
                    for tcb in self.mlfq[lvl]:
                        if now - tcb.arrival_time >= STARVATION_THRESHOLD:
                            self.log(f"[AGING] Promote {tcb.tid[-4:]} Q{lvl}->Q{lvl-1}")
                            tcb.level -= 1
                            tcb.arrival_time = now
                            self.mlfq[tcb.level].append(tcb)
                        else:
                            temp.append(tcb)
                    self.mlfq[lvl] = temp
                self.condition.notify_all()

    def worker_loop(self, wid):
        self.log(f"Worker-{wid} Ready.")
        while True:
            with self.condition:
                while self.running and not any(self.mlfq):
                    self.condition.wait()
                if not self.running and not any(self.mlfq):
                    break
                tcb = self.schedule_next()
                tcb.state = "RUNNING"

            quantum = self.quantum[tcb.level]
            exec_time = min(quantum, tcb.remaining)

            self.log(
                f"[RUNNING] Worker-{wid} {tcb.tid[-4:]} "
                f"{tcb.tx_type} Q{tcb.level} Exec={exec_time}s Rem={tcb.remaining}s"
            )

            time.sleep(exec_time)
            tcb.remaining -= exec_time

            with self.condition:
                if tcb.remaining > 0:
                    if tcb.level < 2:
                        tcb.level += 1
                    tcb.arrival_time = time.time()
                    tcb.state = "READY"
                    self.mlfq[tcb.level].append(tcb)
                else:
                    tcb.state = "TERMINATED"
                    self.log(f"[DONE] {tcb.tid[-4:]}")
                    self.response_queue.put({"id": tcb.tid, "status": "DONE"})

    def run(self):
        self.mlfq = [[], [], []]
        self.quantum = [1, 2, 4]
        self.condition = threading.Condition()
        self.running = True

        self.log("Process Started. Policy: MLFQ | Workers: 1")

        workers = []
        for i in range(MAX_WORKERS):
            t = threading.Thread(target=self.worker_loop, args=(i + 1,))
            t.start()
            workers.append(t)

        aging = threading.Thread(target=self.aging_worker)
        aging.start()

        while True:
            req = self.request_queue.get()
            if req.get("type") == "TERMINATE":
                break

            tcb = ThreadControlBlock(req["request_id"], req["type"])
            with self.condition:
                tcb.state = "READY"
                self.mlfq[0].append(tcb)
                self.log(f"[NEW] {tcb.tx_type} ID={tcb.tid[-4:]} -> Q0")
                self.condition.notify()

        with self.condition:
            self.running = False
            self.condition.notify_all()

        for w in workers:
            w.join()

        aging.join()
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

    print("\nphase 3 + MLFQ done")


if __name__ == "__main__":
    main()
