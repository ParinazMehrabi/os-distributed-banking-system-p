from multiprocessing import Process, Queue
import os
import time
import uuid
from datetime import datetime


class BranchProcess(Process):
    def __init__(self, branch_name, request_queue, response_queue, local_db):
        super().__init__()
        self.branch_name = branch_name
        self.request_queue = request_queue
        self.response_queue = response_queue
        self.local_db = local_db

    def log(self, message):
        print(f"[PROCESS {os.getpid()} - {self.branch_name}] {message}")

    def handle_transfer(self, request):
        from_acc = request["from_account"]
        to_acc = request["to_account"]
        amount = request["amount"]

        self.log(f"Processing transfer: {amount}$ → {to_acc}")
        if to_acc not in self.local_db:
            return {
                "request_id": request["request_id"],
                "type": "TRANSFER_CONFIRMATION",
                "status": "FAILED",
                "reason": "Destination account not found"
            }
        self.local_db[to_acc] += amount

        return {
            "request_id": request["request_id"],
            "type": "TRANSFER_CONFIRMATION",
            "status": "SUCCESS",
            "to_account": to_acc,
            "new_balance": self.local_db[to_acc]
        }

    def run(self):
        self.log("Process Started")

        if self.branch_name == "Digital_Branch":
            time.sleep(1)

            request_id = str(uuid.uuid4())

            request = {
                "request_id": request_id,
                "timestamp": str(datetime.now()),
                "type": "TRANSFER_REQUEST",
                "from_branch": "Digital_Branch",
                "to_branch": "Central_Branch",
                "from_account": "ACC-Joint",
                "to_account": "ACC-A",
                "amount": 500
            }

            self.log("Sending TRANSFER_REQUEST to Central Branch")
            self.request_queue.put(request)
            response = self.response_queue.get()

            if response["request_id"] == request_id:
                if response["status"] == "SUCCESS":
                    self.local_db["ACC-Joint"] -= request["amount"]
                    self.log(f"Transfer Completed| New Balance ACC-Joint: {self.local_db['ACC-Joint']}")
                else:
                    self.log(f"Transfer Failed | Reason: {response.get('reason')}")

        elif self.branch_name == "Central_Branch":
            while True:
                request = self.request_queue.get()

                if request["type"] == "TERMINATE":
                    self.log("Shutting Down...")
                    break

                self.log(f"Received request from {request['from_branch']}")

                if request["type"] == "TRANSFER_REQUEST":
                    response = self.handle_transfer(request)
                    self.response_queue.put(response)


def main():
    print("\n[STEP 1: INFRASTRUCTURE]")
    print("[SYSTEM]: OS Kernel Initialized.")

    request_queue = Queue()
    response_queue = Queue()
    central_db = {
        "ACC-A": 5000,
        "ACC-B": 3000
    }
    digital_db = {
        "ACC-Joint": 1000
    }

    central_branch = BranchProcess(
        "Central_Branch",
        request_queue,
        response_queue,
        central_db
    )

    digital_branch = BranchProcess(
        "Digital_Branch",
        request_queue,
        response_queue,
        digital_db
    )

    central_branch.start()
    digital_branch.start()

    digital_branch.join()
    request_queue.put({"type": "TERMINATE"})
    central_branch.join()

    print("\n[SYSTEM]: Phase 1 Finished Successfully.")
    print("\n-- FINAL BALANCES --")
    print("Central Branch DB:", central_db)
    print("Digital Branch DB:", digital_db)


if __name__ == "__main__":
    main()