from multiprocessing import Process, Queue, set_start_method
import threading
import time
import uuid
import queue
DEADLOCK_CHECK_INTERVAL = 2
MAX_WORKERS = 2
SENTINEL = None
class ThreadControlBlock:
    def __init__(self, tid, tx_type):
        self.tid = tid
        self.tx_type = tx_type
        self.state = "NEW"
        self.held = set()
        self.waiting_for = None
class Account:
    def __init__(self, acc_id, balance):
        self.acc_id = acc_id
        self.balance = balance
        self.lock = threading.Lock()


class BranchProcess(Process):
    def __init__(self, name, req_q):
        super().__init__()
        self.name = name
        self.req_q = req_q

    def log(self, msg):
        print(f"[{self.name}] {msg}", flush=True)
    def bankers_safe_check(self, tid, req):
        work = set(self.available)
        alloc = {k: set(v) for k, v in self.allocation.items()}
        need = {k: self.max_need[k] - alloc[k] for k in alloc}

        if req not in work:
            return False

        work.remove(req)
        alloc[tid].add(req)
        need[tid].remove(req)

        finished = set()
        progress = True

        while progress:
            progress = False
            for t in alloc:
                if t in finished:
                    continue
                if need[t].issubset(work):
                    work |= alloc[t]
                    finished.add(t)
                    progress = True

        return len(finished) == len(alloc)

    def safe_acquire(self, tcb, acc_id):
        while True:
            with self.graph_lock:
                if self.bankers_safe_check(tcb.tid, acc_id):
                    self.available.remove(acc_id)
                    self.allocation[tcb.tid].add(acc_id)
                    break
            time.sleep(0.2)

        self.accounts[acc_id].lock.acquire()

        with self.graph_lock:
            tcb.held.add(acc_id)
            self.resource_owner[acc_id] = tcb.tid

    def safe_release(self, tcb, acc_id):
        with self.graph_lock:
            if acc_id in tcb.held:
                self.accounts[acc_id].lock.release()
                tcb.held.remove(acc_id)
                self.allocation[tcb.tid].remove(acc_id)
                self.available.add(acc_id)
                self.resource_owner.pop(acc_id, None)

    def release_all(self, tcb):
        for acc in list(tcb.held):
            self.safe_release(tcb, acc)

    def transfer(self, tcb, src, dst):
        a = f"ACC-{src}"
        b = f"ACC-{dst}"

        self.safe_acquire(tcb, a)
        time.sleep(1)
        self.safe_acquire(tcb, b)

        self.accounts[a].balance -= 100
        self.accounts[b].balance += 100

        self.safe_release(tcb, b)
        self.safe_release(tcb, a)

    def worker(self):
        while True:
            try:
                tcb = self.ready_q.get(timeout=1)
            except queue.Empty:
                if not self.running:
                    break
                continue

            if tcb is SENTINEL:
                break

            with self.graph_lock:
                self.tcbs[tcb.tid] = tcb
                self.max_need[tcb.tid] = {"ACC-A", "ACC-B"}
                self.allocation[tcb.tid] = set()

            tcb.state = "RUNNING"

            if tcb.tx_type == "TRANSFER_AB":
                self.transfer(tcb, "A", "B")
            else:
                self.transfer(tcb, "B", "A")

            tcb.state = "COMPLETED"
            self.release_all(tcb)
            self.log(f"[SUCCESS] {tcb.tid[-4:]}")

    def run(self):
        self.accounts = {
            "ACC-A": Account("ACC-A", 1000),
            "ACC-B": Account("ACC-B", 1000)
        }

        self.tcbs = {}
        self.resource_owner = {}
        self.ready_q = queue.Queue()
        self.graph_lock = threading.Lock()
        self.running = True

        self.available = set(self.accounts.keys())
        self.max_need = {}
        self.allocation = {}

        workers = []
        for _ in range(MAX_WORKERS):
            t = threading.Thread(target=self.worker)
            t.start()
            workers.append(t)

        while True:
            req = self.req_q.get()
            if req is SENTINEL:
                break
            self.ready_q.put(ThreadControlBlock(req["id"], req["type"]))

        self.running = False

        for _ in workers:
            self.ready_q.put(SENTINEL)

        for w in workers:
            w.join()

        self.log("Process Shutdown Clean")


def main():
    set_start_method("spawn", force=True)

    q = Queue()
    p = BranchProcess("Central", q)
    p.start()

    q.put({"id": str(uuid.uuid4()), "type": "TRANSFER_AB"})
    q.put({"id": str(uuid.uuid4()), "type": "TRANSFER_BA"})

    time.sleep(10)

    q.put(SENTINEL)
    p.join()

    print("\nphase 5 - banker done")


if __name__ == "__main__":
    main()