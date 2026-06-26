from multiprocessing import Process, Queue, Manager
from cryptography.fernet import Fernet
import json
import time
import uuid
from datetime import datetime
import os

SECRET_KEY = b"9X0o2YqX9ZkH8z0z6c2m0wQmZlYzQ2xJ8pQkKxv0ZzQ="
AUTH_TOKEN = "BANK_SECURE_TOKEN"
SENTINEL = b"TERMINATE"

cipher = Fernet(SECRET_KEY)

def encrypt_message(data):
    return cipher.encrypt(json.dumps(data).encode())

def decrypt_message(blob):
    return json.loads(cipher.decrypt(blob).decode())

class BranchProcess(Process):
    def __init__(self, name, req_q, res_q, db):
        super().__init__()
        self.name = name
        self.req_q = req_q
        self.res_q = res_q
        self.db = db

    def log(self, msg):
        print(f"[PROCESS {os.getpid()} - {self.name}] {msg}", flush=True)

    def run(self):
        self.log("Process Started")

        if self.name == "Digital_Branch":
            time.sleep(1)

            payload = {
                "auth": AUTH_TOKEN,
                "request_id": str(uuid.uuid4()),
                "type": "TRANSFER_REQUEST",
                "from_account": "ACC-Joint",
                "to_account": "ACC-A",
                "amount": 500,
                "timestamp": str(datetime.now())
            }

            enc = encrypt_message(payload)
            self.log("Encrypted TRANSFER_REQUEST sent via IPC")
            self.req_q.put(enc)

            try:
                enc_resp = self.res_q.get(timeout=5)
                resp = decrypt_message(enc_resp)
            except:
                self.log("Response Timeout or Corrupted")
                return

            if resp["status"] == "SUCCESS":
                self.log(f"Transfer Completed | ACC-Joint Balance: {self.db['ACC-Joint']}")
            else:
                self.log("Transfer Failed")

        else:
            while True:
                msg = self.req_q.get()

                if msg == SENTINEL:
                    self.log("Shutting Down Cleanly")
                    break

                try:
                    req = decrypt_message(msg)
                except:
                    self.log("Decryption Failed – Packet Dropped")
                    continue

                if req.get("auth") != AUTH_TOKEN:
                    self.log("Authentication Failed")
                    continue

                if req["type"] == "TRANSFER_REQUEST":
                    from_acc = req["from_account"]
                    to_acc = req["to_account"]
                    amt = req["amount"]

                    if (
                        from_acc not in self.db or
                        to_acc not in self.db or
                        amt <= 0 or
                        self.db[from_acc] < amt
                    ):
                        resp = {"status": "FAILED"}
                    else:
                        self.db[from_acc] -= amt
                        self.db[to_acc] += amt
                        resp = {
                            "status": "SUCCESS",
                            "new_balance": self.db[to_acc]
                        }

                    self.log("Secure Request Processed")
                    self.res_q.put(encrypt_message(resp))

def main():
    print("\n[STEP 1: INFRASTRUCTURE]")
    print("[SYSTEM]: OS Kernel Initialized.", flush=True)

    manager = Manager()
    shared_db = manager.dict({
        "ACC-A": 5000,
        "ACC-B": 3000,
        "ACC-Joint": 1000
    })

    req_q = Queue()
    res_q = Queue()

    central = BranchProcess("Central_Branch", req_q, res_q, shared_db)
    digital = BranchProcess("Digital_Branch", req_q, res_q, shared_db)

    central.start()
    digital.start()

    digital.join()
    req_q.put(SENTINEL)
    central.join()

    print("\n[SYSTEM]: Phase 1 + Bonus 5 Completed")
    print("Final DB State:", dict(shared_db))

if __name__ == "__main__":
    main()
