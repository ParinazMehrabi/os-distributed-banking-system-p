from multiprocessing import Process, Queue, set_start_method
import threading
import time
import uuid
import heapq
TX_BURST = {
    "BALANCE": 1,
    "WITHDRAW": 2, 
    "DEPOSIT": 2,
    "HEAVY_CALC": 5
}

AGING_INTERVAL = 3
STARVATION_THRESHOLD = 8
MAX_WORKERS = 2 
class ThreadControlBlock:
    def __init__(self, tid, tx_type, account_target=None, amount=0):
        self.tid = tid
        self.tx_type = tx_type
        self.account_target = account_target
        self.amount = amount
        self.burst_time = TX_BURST.get(tx_type, 2)
        self.priority = self.burst_time
        self.arrival_time = time.time()
        self.state = "NEW"

    def __lt__(self, other):
        if self.priority == other.priority:
            return self.arrival_time < other.arrival_time
        return self.priority < other.priority

class Account:
    def __init__(self, acc_id, balance):
        self.acc_id = acc_id
        self.balance = balance
        self.lock = threading.Lock() 

class BranchProcess(Process):
    def __init__(self, name, req_q, res_q):
        super().__init__()
        self.name = name
        self.req_q = req_q
        self.res_q = res_q

    def log(self, msg):
        print(f"[{self.name}] {msg}")
    def aging_worker(self):
        while True:
            with self.cond:
                self.cond.wait(timeout=AGING_INTERVAL)
                if not self.running and not self.ready_q: break
                
                now = time.time()
                temp = []
                while self.ready_q:
                    tcb = heapq.heappop(self.ready_q)
                    if (now - tcb.arrival_time > STARVATION_THRESHOLD) and tcb.priority > 1:
                        tcb.priority -= 1
                        self.log(f"[AGING] Boost {tcb.tid[-4:]} -> Prio {tcb.priority}")
                    temp.append(tcb)
                
                for t in temp: heapq.heappush(self.ready_q, t)
                self.cond.notify_all()
    def worker_loop(self, wid):
        self.log(f"Worker-{wid} Ready")
        while True:
            with self.cond:
                while self.running and not self.ready_q:
                    self.cond.wait()
                if not self.running and not self.ready_q: break
                
                tcb = heapq.heappop(self.ready_q)
                tcb.state = "RUNNING"
            
            self.execute_transaction(tcb, wid)
    def execute_transaction(self, tcb, wid):
        self.log(f"[RUN] Worker-{wid} started {tcb.tid[-4:]} ({tcb.tx_type})")
        if tcb.account_target and tcb.account_target in self.accounts:
            acc = self.accounts[tcb.account_target]
            tcb.state = "BLOCKED"
            self.log(f"[MUTEX] {tcb.tid[-4:]} Requesting Lock on {acc.acc_id} (Status: BLOCKED)")
            with acc.lock:
                tcb.state = "RUNNING"
                self.log(f"[CRITICAL] {tcb.tid[-4:]} Acquired Lock on {acc.acc_id}")
                time.sleep(2) 
                if tcb.tx_type == "WITHDRAW":
                    if acc.balance >= tcb.amount:
                        acc.balance -= tcb.amount
                        self.log(f"[SUCCESS] {tcb.tid[-4:]} Withdrew {tcb.amount}. New Balance: {acc.balance}")
                    else:
                        self.log(f"[FAILED] {tcb.tid[-4:]} Insufficient Funds! Balance: {acc.balance}")
                
                self.log(f"[RELEASE] {tcb.tid[-4:]} Releasing Lock")
        
        else:
            time.sleep(tcb.burst_time)

        tcb.state = "TERMINATED"
        self.res_q.put({"id": tcb.tid, "status": "DONE"})

    def run(self):
        self.ready_q = []
        self.cond = threading.Condition()
        self.running = True
        self.accounts = {
            "ACC-JOINT": Account("ACC-JOINT", 1000) 
        }

        self.log("System Started. Mutex/Locking Active.")

        workers = []
        for i in range(MAX_WORKERS):
            t = threading.Thread(target=self.worker_loop, args=(i+1,))
            t.start()
            workers.append(t)

        aging = threading.Thread(target=self.aging_worker)
        aging.start()

        while True:
            req = self.req_q.get()
            if req.get("type") == "TERMINATE": break
            
            tcb = ThreadControlBlock(
                req["id"], req["type"], 
                req.get("account"), req.get("amount")
            )
            
            with self.cond:
                tcb.state = "READY"
                heapq.heappush(self.ready_q, tcb)
                self.cond.notify()

        with self.cond:
            self.running = False
            self.cond.notify_all()

        for w in workers: w.join()
        aging.join()
        self.log("System Stopped.")

def main():
    set_start_method("spawn", force=True)
    req_q = Queue()
    res_q = Queue()

    p = BranchProcess("Central_Branch", req_q, res_q)
    p.start()
    husband_id = str(uuid.uuid4())
    wife_id = str(uuid.uuid4())
    print("\n--- Sending Husband Request (Withdraw 1000) ---")
    req_q.put({
        "id": husband_id, "type": "WITHDRAW", 
        "account": "ACC-JOINT", "amount": 1000
    })
    time.sleep(0.1) 
    
    print("--- Sending Wife Request (Withdraw 1000) ---")
    req_q.put({
        "id": wife_id, "type": "WITHDRAW", 
        "account": "ACC-JOINT", "amount": 1000
    })

    time.sleep(6) 
    req_q.put({"type": "TERMINATE"})
    p.join()
    print("\nPhase 4 (Mutex) Completed.")

if __name__ == "__main__":
    main()