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
        self.abort = False
        self.retries = 0


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

    def detect_deadlock(self):
        graph = {}
        with self.graph_lock:
            for t in self.tcbs.values():
                if t.waiting_for:
                    owner = self.resource_owner.get(t.waiting_for)
                    if owner and owner != t.tid:
                        graph[t.tid] = owner

        def dfs(node, path):
            if node in path:
                return path[path.index(node):]
            if node not in graph:
                return None
            return dfs(graph[node], path + [node])

        for n in graph:
            cycle = dfs(n, [])
            if cycle:
                return cycle
        return None

    def recovery(self, cycle):
        with self.graph_lock:
            victims = [self.tcbs[t] for t in cycle if t in self.tcbs]
            if not victims:
                return
            victim = min(victims, key=lambda x: (len(x.held), x.retries))
            victim.abort = True
            self.log(f"[RECOVERY] Victim {victim.tid[-4:]} Aborted")

    def deadlock_monitor(self):
        while self.running:
            time.sleep(DEADLOCK_CHECK_INTERVAL)
            cycle = self.detect_deadlock()
            if cycle:
                chain = " -> ".join(t[-4:] for t in cycle)
                self.log(f"[DEADLOCK] Cycle Detected: {chain}")
                self.recovery(cycle)

    def safe_acquire(self, tcb, acc_id):
        acc = self.accounts[acc_id]
        with self.graph_lock:
            if tcb.abort:
                raise RuntimeError
            tcb.waiting_for = acc_id

        while True:
            if tcb.abort:
                with self.graph_lock:
                    tcb.waiting_for = None
                raise RuntimeError
            if acc.lock.acquire(timeout=0.5):
                break

        with self.graph_lock:
            tcb.waiting_for = None
            tcb.held.add(acc_id)
            self.resource_owner[acc_id] = tcb.tid

    def safe_release(self, tcb, acc_id):
        with self.graph_lock:
            if acc_id in tcb.held:
                try:
                    self.accounts[acc_id].lock.release()
                except:
                    pass
                tcb.held.remove(acc_id)
                if self.resource_owner.get(acc_id) == tcb.tid:
                    del self.resource_owner[acc_id]

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

            tcb.abort = False
            tcb.state = "RUNNING"

            try:
                if tcb.tx_type == "TRANSFER_AB":
                    self.transfer(tcb, "A", "B")
                else:
                    self.transfer(tcb, "B", "A")
                tcb.state = "COMPLETED"
                self.log(f"[SUCCESS] {tcb.tid[-4:]}")
            except RuntimeError:
                self.log(f"[ROLLBACK] {tcb.tid[-4:]}")
                self.release_all(tcb)
                tcb.retries += 1
                tcb.state = "READY"
                time.sleep(1)
                self.ready_q.put(tcb)

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

        monitor = threading.Thread(target=self.deadlock_monitor)
        monitor.start()

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

        monitor.join()
        self.log("Process Shutdown Clean")


def main():
    set_start_method("spawn", force=True)

    q = Queue()
    p = BranchProcess("Central", q)
    p.start()

    q.put({"id": str(uuid.uuid4()), "type": "TRANSFER_AB"})
    q.put({"id": str(uuid.uuid4()), "type": "TRANSFER_BA"})

    time.sleep(15)

    q.put(SENTINEL)
    p.join()

    print("\nPhase 5 Completed Successfully")


if __name__ == "__main__":
    main()
