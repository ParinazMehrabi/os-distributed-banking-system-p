from multiprocessing import Process, Queue, set_start_method
import threading
import time
import uuid
import os
import random
class ThreadControlBlock:
    def __init__(self, tid, tx_type):
        self.tid = tid
        self.tx_type = tx_type
        self.state = "NEW"
class BranchProcess(Process):

    def __init__(self, branch_name, request_queue, response_queue, local_db):
        super().__init__()
        self.branch_name = branch_name
        self.request_queue = request_queue
        self.response_queue = response_queue
        self.local_db = local_db
        self.locks = None
        self.scheduler_lock = None
        self.total_balance_lock = None

        self.total_balance = 0
        self.tcb_table = {}
        self.ready_queue = []
        self.threads = []
        self.running = True
    def log(self, msg):
        print(f"[PID {os.getpid()} | {self.branch_name}] {msg}")
    def scheduler_loop(self):
        while self.running or self.ready_queue:

            with self.scheduler_lock:
                if self.ready_queue:
                    req = self.ready_queue.pop(0)
                else:
                    req = None

            if req:
                t = threading.Thread(
                    target=self.transaction_thread,
                    args=(req,),
                    daemon=False
                )
                t.start()
                self.threads.append(t)
            else:
                time.sleep(0.05)
    def transaction_thread(self, request):
        tid = request["request_id"]
        tcb = ThreadControlBlock(tid, request["type"])
        self.tcb_table[tid] = tcb
        self.log(f"[TCB-{tid[-4:]}] STATE → NEW")
        tcb.state = "READY"
        self.log(f"[TCB-{tid[-4:]}] STATE → READY")
        tcb.state = "RUNNING"
        self.log(f"[TCB-{tid[-4:]}] STATE → RUNNING")
        tx_type = request["type"]
        acc = request["account"]
        amount = request.get("amount", 0)
        tcb.state = "BLOCKED"
        self.log(f"[TCB-{tid[-4:]}] WAITING FOR LOCK on {acc}")

        with self.locks[acc]:

            tcb.state = "RUNNING"
            self.log(f"[TCB-{tid[-4:]}] LOCK ACQUIRED on {acc}")

            old_balance = self.local_db[acc]
            time.sleep(0.5)

            success = True

            if tx_type == "DEPOSIT":
                self.local_db[acc] += amount
                with self.total_balance_lock:
                    self.total_balance += amount

            elif tx_type == "WITHDRAW":
                if self.local_db[acc] >= amount:
                    self.local_db[acc] -= amount
                    with self.total_balance_lock:
                        self.total_balance -= amount
                else:
                    success = False
                    self.log(f"[TCB-{tid[-4:]}]Insufficient Funds")

            elif tx_type == "TRANSFER":
                self.local_db[acc] += amount
                with self.total_balance_lock:
                    self.total_balance += amount

            elif tx_type == "BALANCE":
                pass

            new_balance = self.local_db[acc]

            if success:
                self.log(
                    f"[TCB-{tid[-4:]}]{tx_type} {acc}: "
                    f"{old_balance} → {new_balance} | Total: {self.total_balance}"
                )
        tcb.state = "TERMINATED"
        self.log(f"[TCB-{tid[-4:]}] STATE → TERMINATED")
        self.response_queue.put({"request_id": tid, "status": "DONE"})
    def run(self):

        self.locks = {acc: threading.Lock() for acc in self.local_db}
        self.scheduler_lock = threading.Lock()
        self.total_balance_lock = threading.Lock()

        self.total_balance = sum(self.local_db.values())

        self.log(f"Process Started | Initial Total: {self.total_balance}")

        scheduler_thread = threading.Thread(
            target=self.scheduler_loop,
            daemon=True
        )
        scheduler_thread.start()
        if self.branch_name == "Digital_Branch":

            time.sleep(1)

            request_ids = []

            for _ in range(8):
                rid = str(uuid.uuid4())
                req = {
                    "request_id": rid,
                    "type": random.choice(
                        ["DEPOSIT", "WITHDRAW", "TRANSFER", "BALANCE"]
                    ),
                    "account": random.choice(["ACC-A", "ACC-B"]),
                    "amount": random.randint(50, 100)
                }

                request_ids.append(rid)

                self.log(f"snding Req {rid[-4:]}")
                self.request_queue.put(req)

            for _ in request_ids:
                self.log("WAITING FOR IPC RESPONSE (BLOCKED)")
                resp = self.response_queue.get()
                self.log(f"IPC RESPONSE RECEIVED → {resp['request_id'][-4:]}")

            self.request_queue.put({"type": "TERMINATE"})
        else:
            while True:
                req = self.request_queue.get()

                if req.get("type") == "TERMINATE":
                    break

                with self.scheduler_lock:
                    self.ready_queue.append(req)
            self.running = False
            for t in self.threads:
                t.join()
            self.log(f"Final Branch Total Balance: {self.total_balance}")
            self.log(f"Total Transactions Processed: {len(self.tcb_table)}")
        self.log("Process Terminated")

def main():

    set_start_method("spawn", force=True)

    request_q = Queue()
    response_q = Queue()

    central_db = {"ACC-A": 5000, "ACC-B": 3000}

    central = BranchProcess(
        "Central_Branch", request_q, response_q, central_db
    )

    digital = BranchProcess(
        "Digital_Branch", request_q, response_q,
        {"ACC-A": 0, "ACC-B": 0}
    )

    central.start()
    digital.start()

    digital.join()
    central.join()

    print("\nphase 2 done")


if __name__ == "__main__":
    main()