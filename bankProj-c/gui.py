import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import time
import uuid
import queue
import heapq
import os
from datetime import datetime
import random


TX_BURST = {
    "BALANCE": 1,
    "DEPOSIT": 2,
    "WITHDRAW": 2,
    "TRANSFER": 4,
    "HEAVY_CALC": 8,
    "ANNUAL_PROFIT": 10
}

AGING_INTERVAL = 2
STARVATION_THRESHOLD = 6
MAX_WORKERS = 2
DEADLOCK_CHECK_INTERVAL = 2


class ThreadControlBlock:
    def __init__(self, tid, tx_type, account=None, amount=0, from_account=None, to_account=None):
        self.tid = tid
        self.tx_type = tx_type
        self.account = account
        self.amount = amount
        self.from_account = from_account
        self.to_account = to_account
        self.burst_time = TX_BURST.get(tx_type, 2)
        self.priority = self.burst_time
        self.arrival_time = time.time()
        self.state = "NEW"
        self.wait_start = None
        self.held_locks = set()
        self.waiting_for = None
        self.abort = False
        self.retries = 0
        self.branch = None
        
    def __lt__(self, other):
        if self.priority == other.priority:
            return self.arrival_time < other.arrival_time
        return self.priority < other.priority

class Account:
    def __init__(self, acc_id, balance, branch="Central"):
        self.acc_id = acc_id
        self.balance = balance
        self.lock = threading.Lock()
        self.branch = branch


class DistributedBankSystem:
    def __init__(self, gui_callback=None):
        self.gui_callback = gui_callback
        self.accounts = {}
        self.processes = {}
        self.queues = {}
        self.tcbs = {}
        self.ready_queue = []
        self.resource_owner = {}
        self.graph_lock = threading.Lock()
        self.running = False
        self.condition = threading.Condition()
        self.workers = []
        self.monitor_thread = None
        self.aging_thread = None
        self.total_balance = 0
        self.total_balance_lock = threading.Lock()
        self.transaction_count = 0
        self.successful_tx = 0
        self.failed_tx = 0
        self.deadlocks_resolved = 0
        self.starvation_cases = 0
        self.system_lock = threading.Lock()
        
    def log(self, message):
        if self.gui_callback:
            self.gui_callback(message)
        else:
            print(message)
            
  #phase 1
    def setup_infrastructure(self, branches=None):
        self.log("[STEP 1: INFRASTRUCTURE]")
        self.log("[SYSTEM]: OS Kernel Initialized.")
        
        if not branches:
            branches = {
                "Central_Branch": {"ACC-A": 5000, "ACC-B": 3000},
                "Digital_Branch": {"ACC-Joint": 1000}
            }
        
        for branch_name, accounts in branches.items():
            for acc_id, balance in accounts.items():
                self.accounts[acc_id] = Account(acc_id, balance, branch_name)
            
            self.log(f"[PROCESS {os.getpid()}]: {branch_name} Process Started.")
        
        self.log("[IPC]: Message Queue & Shared Memory Linked.")
        return self.accounts
    
  #phase 2
    def aging_worker(self):
        while self.running:
            with self.condition:
                self.condition.wait(timeout=AGING_INTERVAL)
                if not self.running:
                    break
                    
                now = time.time()
                temp = []
                changed = False
                
                items = []
                while self.ready_queue:
                    items.append(heapq.heappop(self.ready_queue))
                
                for tcb in items:
                    wait_time = now - tcb.arrival_time
                    
                    if wait_time > STARVATION_THRESHOLD and tcb.priority > 1:
                        self.log(f"[AGING-MONITOR] Starvation Detected: {tcb.tx_type} (Wait: {int(wait_time)}s)")
                        tcb.priority = max(1, tcb.priority - 2)
                        tcb.arrival_time = now
                        self.log(f"[AGING-APPLIED] {tcb.tx_type} New Priority: {tcb.priority}")
                        self.starvation_cases += 1
                        changed = True
                    
                    temp.append(tcb)
                
                for t in temp:
                    heapq.heappush(self.ready_queue, t)
                
                if changed:
                    self.condition.notify_all()
    
 #phase 4
    def acquire_lock_safe(self, tcb, acc_id):
        if acc_id not in self.accounts:
            return True
            
        acc = self.accounts[acc_id]
        
        with self.graph_lock:
            tcb.waiting_for = acc_id
            tcb.state = "BLOCKED"
            self.log(f"[MUTEX] {tcb.tx_type} waiting for {acc_id}")
        
        result = acc.lock.acquire(timeout=3)
        
        with self.graph_lock:
            tcb.waiting_for = None
            if result:
                tcb.held_locks.add(acc_id)
                self.resource_owner[acc_id] = tcb.tid
                tcb.state = "RUNNING"
                self.log(f"[MUTEX] {tcb.tx_type} acquired {acc_id}")
            else:
                tcb.state = "READY"
                
        return result
    
    def release_lock_safe(self, tcb, acc_id):
        if acc_id in tcb.held_locks and acc_id in self.accounts:
            try:
                self.accounts[acc_id].lock.release()
                tcb.held_locks.remove(acc_id)
                if self.resource_owner.get(acc_id) == tcb.tid:
                    del self.resource_owner[acc_id]
                self.log(f"[MUTEX] {tcb.tx_type} released {acc_id}")
            except:
                pass
    
    def release_all_locks(self, tcb):
        for acc_id in list(tcb.held_locks):
            self.release_lock_safe(tcb, acc_id)
    
 #phase 5
    def detect_deadlock(self):
        graph = {}
        with self.graph_lock:
            for tcb in self.tcbs.values():
                if tcb.waiting_for and tcb.tid in self.tcbs:
                    owner = self.resource_owner.get(tcb.waiting_for)
                    if owner and owner != tcb.tid:
                        graph[tcb.tid] = owner
        
        def dfs(node, path):
            if node in path:
                return path[path.index(node):]
            if node not in graph:
                return None
            return dfs(graph[node], path + [node])
        
        for node in graph:
            cycle = dfs(node, [])
            if cycle:
                return cycle
        return None
    
    def deadlock_monitor(self):
        while self.running:
            time.sleep(DEADLOCK_CHECK_INTERVAL)
            cycle = self.detect_deadlock()
            if cycle:
                self.log(f"[DEADLOCK] Cycle Detected!")
                self.resolve_deadlock(cycle)
    
    def resolve_deadlock(self, cycle):
        with self.graph_lock:
            victims = [self.tcbs[t] for t in cycle if t in self.tcbs]
            if not victims:
                return
            
            victim = min(victims, key=lambda x: (len(x.held_locks), x.retries))
            victim.abort = True
            victim_id = victim.tid[-4:]
            self.log(f"[RECOVERY] Victim selected: {victim.tx_type} ({victim_id})")
            

            for acc in list(victim.held_locks):
                self.log(f"[RECOVERY] Releasing {acc}")
            
            self.deadlocks_resolved += 1
    
    #Transaction Execution
    def execute_transaction(self, tcb, worker_id):
        tx_id = tcb.tid[-4:]
        self.log(f"[WORKER-{worker_id}] Started: {tcb.tx_type}")
        
        try:
            if tcb.abort:
                raise RuntimeError("Aborted")
            
            if tcb.tx_type == "TRANSFER" and tcb.from_account and tcb.to_account:
                tx_id = tcb.tid[-4:]
                self.log(f"[TRANSFER-{tx_id}] Attempting {tcb.amount}$ from {tcb.from_account} to {tcb.to_account}")
                if self.acquire_lock_safe(tcb, tcb.from_account):
                    time.sleep(0.5)
                    if tcb.abort:
                        raise RuntimeError("Aborted")
                    
                    if self.acquire_lock_safe(tcb, tcb.to_account):
                        from_acc = self.accounts[tcb.from_account]
                        to_acc = self.accounts[tcb.to_account]
                        
                        if from_acc.balance >= tcb.amount:
                            from_acc.balance -= tcb.amount
                            to_acc.balance += tcb.amount
                            self.log(f"[TRANSFER] {tcb.amount}$ from {tcb.from_account} to {tcb.to_account}")
                            
                            with self.total_balance_lock:
                                self.total_balance = sum(acc.balance for acc in self.accounts.values())
                            
                            self.successful_tx += 1
                        else:
                            self.log(f"[FAILED] Insufficient funds in {tcb.from_account}")
                            self.failed_tx += 1
                        
                        self.release_lock_safe(tcb, tcb.to_account)
                    
                    self.release_lock_safe(tcb, tcb.from_account)
                else:
                    self.log(f"[FAILED] Could not acquire lock for {tcb.from_account}")
                    self.failed_tx += 1
                    
            elif tcb.tx_type in ["WITHDRAW", "DEPOSIT"] and tcb.account:
                if self.acquire_lock_safe(tcb, tcb.account):
                    acc = self.accounts[tcb.account]
                    old_balance = acc.balance
                    
                    time.sleep(tcb.burst_time)
                    
                    if tcb.tx_type == "WITHDRAW":
                        if acc.balance >= tcb.amount:
                            acc.balance -= tcb.amount
                            self.log(f"[WITHDRAW] {tcb.account}: {old_balance} → {acc.balance}")
                            self.successful_tx += 1
                        else:
                            self.log(f"[FAILED] Insufficient funds in {tcb.account}")
                            self.failed_tx += 1
                    else:  
                        acc.balance += tcb.amount
                        self.log(f"[DEPOSIT] {tcb.account}: {old_balance} → {acc.balance}")
                        self.successful_tx += 1
                    
                    with self.total_balance_lock:
                        self.total_balance = sum(acc.balance for acc in self.accounts.values())
                    
                    self.release_lock_safe(tcb, tcb.account)
                else:
                    self.log(f"[FAILED] Could not acquire lock for {tcb.account}")
                    self.failed_tx += 1
                    
            elif tcb.tx_type == "BALANCE" and tcb.account:
                if self.acquire_lock_safe(tcb, tcb.account):
                    acc = self.accounts[tcb.account]
                    self.log(f"[BALANCE] {tcb.account}: {acc.balance}$")
                    self.release_lock_safe(tcb, tcb.account)
                    self.successful_tx += 1
                else:
                    self.failed_tx += 1
                    
            else:
                time.sleep(tcb.burst_time)
                self.log(f"[DONE] {tcb.tx_type} completed")
                self.successful_tx += 1
                
        except RuntimeError:
            self.log(f"[ROLLBACK] {tcb.tx_type} aborted")
            self.release_all_locks(tcb)
            self.failed_tx += 1
        except Exception as e:
            self.log(f"[ERROR] {tcb.tx_type}: {str(e)}")
            self.failed_tx += 1
        finally:
            tcb.state = "TERMINATED"
            self.transaction_count += 1
    
    def worker_loop(self, worker_id):
        self.log(f"[WORKER-{worker_id}] Ready")
        
        while self.running:
            tcb = None
            with self.condition:
                try:
                    while self.running and not self.ready_queue:
                        self.condition.wait(timeout=0.5)
                    
                    if not self.running:
                        break
                    
                    if self.ready_queue:
                        tcb = heapq.heappop(self.ready_queue)
                        tcb.state = "RUNNING"
                        self.log(f"[SCHEDULER] Dequeued: {tcb.tx_type} (Burst: {tcb.burst_time}s)")
                except:
                    break
            
            if tcb:
                self.execute_transaction(tcb, worker_id)
    
    # Add New Transaction 
    def add_transaction(self, tx_type, account=None, amount=0, from_account=None, to_account=None):
        tid = str(uuid.uuid4())[:8]
        tcb = ThreadControlBlock(tid, tx_type, account, amount, from_account, to_account)
        
        with self.condition:
            self.tcbs[tid] = tcb
            tcb.state = "READY"
            heapq.heappush(self.ready_queue, tcb)
            self.log(f"[NEW] {tx_type} added to queue (Burst: {tcb.burst_time}s)")
            self.condition.notify()
        
        return tid
    

    def start_system(self):
        with self.system_lock:
            if self.running:
                return
            
            self.running = True
            self.ready_queue = []
            self.tcbs = {}
            self.transaction_count = 0
            self.successful_tx = 0
            self.failed_tx = 0
            self.deadlocks_resolved = 0
            self.starvation_cases = 0
            

            self.workers = []
            for i in range(MAX_WORKERS):
                t = threading.Thread(target=self.worker_loop, args=(i+1,))
                t.daemon = True
                t.start()
                self.workers.append(t)
            

            self.aging_thread = threading.Thread(target=self.aging_worker)
            self.aging_thread.daemon = True
            self.aging_thread.start()
            

            self.monitor_thread = threading.Thread(target=self.deadlock_monitor)
            self.monitor_thread.daemon = True
            self.monitor_thread.start()
            
            self.log("[SYSTEM] Started with SJF + Aging Scheduling")
            self.log(f"[SYSTEM] Workers: {MAX_WORKERS}, Aging Interval: {AGING_INTERVAL}s")
    

    def stop_system(self):
        with self.system_lock:
            if not self.running:
                return
            
            self.log("[SYSTEM] Stopping system...")
            self.running = False
            
            with self.condition:
                self.condition.notify_all()
            
            time.sleep(2)
            
            
            self.log("\n" + "="*50)
            self.log("FINAL SUMMARY")
            self.log("="*50)
            self.log(f"Total Transactions: {self.transaction_count}")
            self.log(f"Successful: {self.successful_tx}")
            self.log(f"Failed: {self.failed_tx}")
            self.log(f"Deadlocks Resolved: {self.deadlocks_resolved}")
            self.log(f"Starvation Cases: {self.starvation_cases}")
            
            final_balances = {acc_id: acc.balance for acc_id, acc in self.accounts.items()}
            self.log(f"Final Balances: {final_balances}")
            self.log("[SYSTEM] System stopped")

# gui
class BankSystemGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Distributed Bank System - Resource Management Simulation")
        self.root.geometry("1200x700")
        self.root.configure(bg='#f0f0f0')
        
        self.system = None
        self.running = False
        self.system_thread = None
        
        self.setup_ui()
        
    def setup_ui(self):
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=5)
        
        self.main_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.main_frame, text='🏦 Main Control')
        
        self.accounts_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.accounts_frame, text='💰 Account Management')
        
        self.transaction_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.transaction_frame, text='💳 Create Transaction')
        
        self.scenarios_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.scenarios_frame, text='📋 Test Scenarios')
        
        self.reports_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.reports_frame, text='📊 Reports')
        
        self.setup_main_tab()
        self.setup_accounts_tab()
        self.setup_transaction_tab()
        self.setup_scenarios_tab()
        self.setup_reports_tab()
    
    def setup_main_tab(self):
        control_frame = ttk.LabelFrame(self.main_frame, text="System Control", padding=10)
        control_frame.pack(fill='x', padx=10, pady=5)
        
        btn_frame = ttk.Frame(control_frame)
        btn_frame.pack()
        
        self.start_btn = ttk.Button(btn_frame, text="▶️ Start System", command=self.start_system, width=20)
        self.start_btn.grid(row=0, column=0, padx=5)
        
        self.stop_btn = ttk.Button(btn_frame, text="⏹️ Stop System", command=self.stop_system, width=20, state='disabled')
        self.stop_btn.grid(row=0, column=1, padx=5)
        
        self.clear_btn = ttk.Button(btn_frame, text="🧹 Clear Output", command=self.clear_output, width=20)
        self.clear_btn.grid(row=0, column=2, padx=5)
        

        status_frame = ttk.LabelFrame(self.main_frame, text="System Status", padding=10)
        status_frame.pack(fill='x', padx=10, pady=5)
        
        self.status_text = tk.StringVar()
        self.status_text.set("⏸️ System Stopped")
        status_label = ttk.Label(status_frame, textvariable=self.status_text, font=('Tahoma', 11, 'bold'))
        status_label.pack()
        

        output_frame = ttk.LabelFrame(self.main_frame, text="System Output", padding=10)
        output_frame.pack(fill='both', expand=True, padx=10, pady=5)
        
        self.output_text = scrolledtext.ScrolledText(
            output_frame, 
            height=20, 
            font=('Consolas', 10),
            bg='white',
            wrap=tk.WORD
        )
        self.output_text.pack(fill='both', expand=True)
    
    def setup_accounts_tab(self):
        add_frame = ttk.LabelFrame(self.accounts_frame, text="Create New Account", padding=10)
        add_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(add_frame, text="Account ID:").grid(row=0, column=0, padx=5, pady=5)
        self.acc_id_entry = ttk.Entry(add_frame, width=15)
        self.acc_id_entry.grid(row=0, column=1, padx=5, pady=5)
        
        ttk.Label(add_frame, text="Balance:").grid(row=0, column=2, padx=5, pady=5)
        self.acc_balance_entry = ttk.Entry(add_frame, width=15)
        self.acc_balance_entry.grid(row=0, column=3, padx=5, pady=5)
        
        ttk.Label(add_frame, text="Branch:").grid(row=0, column=4, padx=5, pady=5)
        self.acc_branch_combo = ttk.Combobox(add_frame, values=["Central_Branch", "Digital_Branch"], width=15)
        self.acc_branch_combo.grid(row=0, column=5, padx=5, pady=5)
        self.acc_branch_combo.set("Central_Branch")
        
        ttk.Button(add_frame, text="➕ Create Account", command=self.add_account).grid(row=0, column=6, padx=5, pady=5)
        
        list_frame = ttk.LabelFrame(self.accounts_frame, text="Accounts List", padding=10)
        list_frame.pack(fill='both', expand=True, padx=10, pady=5)
        
        columns = ('account', 'balance', 'branch')
        self.accounts_tree = ttk.Treeview(list_frame, columns=columns, show='headings', height=15)
        
        self.accounts_tree.heading('account', text='Account Number')
        self.accounts_tree.heading('balance', text='Balance')
        self.accounts_tree.heading('branch', text='Branch')
        
        self.accounts_tree.column('account', width=150)
        self.accounts_tree.column('balance', width=150)
        self.accounts_tree.column('branch', width=200)
        
        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.accounts_tree.yview)
        self.accounts_tree.configure(yscrollcommand=scrollbar.set)
        
        self.accounts_tree.pack(side=tk.LEFT, fill='both', expand=True)
        scrollbar.pack(side=tk.RIGHT, fill='y')
        

        ttk.Button(list_frame, text="🔄 Refresh", command=self.refresh_accounts_list).pack(pady=5)
    
    def setup_transaction_tab(self):

        type_frame = ttk.LabelFrame(self.transaction_frame, text="Transaction Type", padding=10)
        type_frame.pack(fill='x', padx=10, pady=5)
        
        self.tx_type = tk.StringVar(value="TRANSFER")
        
        types = [
            ("Transfer", "TRANSFER"),
            ("Deposit", "DEPOSIT"),
            ("Withdraw", "WITHDRAW"),
            ("Balance Inquiry", "BALANCE"),
            ("Heavy Calculation", "HEAVY_CALC"),
            ("Annual Profit", "ANNUAL_PROFIT")
        ]
        
        for i, (text, value) in enumerate(types):
            ttk.Radiobutton(
                type_frame, 
                text=text, 
                variable=self.tx_type, 
                value=value
            ).grid(row=i//3, column=i%3, padx=10, pady=5, sticky='w')
        

        details_frame = ttk.LabelFrame(self.transaction_frame, text="Transaction Details", padding=10)
        details_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(details_frame, text="From Account:").grid(row=0, column=0, padx=5, pady=5)
        self.from_account_entry = ttk.Entry(details_frame, width=15)
        self.from_account_entry.grid(row=0, column=1, padx=5, pady=5)
        self.from_account_entry.insert(0, "ACC-A")
        
        ttk.Label(details_frame, text="To Account:").grid(row=0, column=2, padx=5, pady=5)
        self.to_account_entry = ttk.Entry(details_frame, width=15)
        self.to_account_entry.grid(row=0, column=3, padx=5, pady=5)
        self.to_account_entry.insert(0, "ACC-B")
        
        ttk.Label(details_frame, text="Amount:").grid(row=1, column=0, padx=5, pady=5)
        self.amount_entry = ttk.Entry(details_frame, width=15)
        self.amount_entry.grid(row=1, column=1, padx=5, pady=5)
        self.amount_entry.insert(0, "100")
        
        ttk.Label(details_frame, text="Account (for W/D):").grid(row=1, column=2, padx=5, pady=5)
        self.account_entry = ttk.Entry(details_frame, width=15)
        self.account_entry.grid(row=1, column=3, padx=5, pady=5)
        self.account_entry.insert(0, "ACC-Joint")
        
        ttk.Button(details_frame, text="✅ Execute Transaction", command=self.execute_transaction).grid(row=2, column=0, columnspan=4, pady=10)
    
    def setup_scenarios_tab(self):
        scenarios = [
            ("Basic Scenario (2 Branches)", self.run_basic_scenario),
            ("SJF & Aging Test", self.run_sjf_scenario),
            ("Mutex Test (Simultaneous Withdrawal)", self.run_mutex_scenario),
            ("Deadlock Test", self.run_deadlock_scenario),
            ("Comprehensive Project Scenario", self.run_comprehensive_scenario)
        ]
        
        for i, (text, command) in enumerate(scenarios):
            btn = ttk.Button(
                self.scenarios_frame, 
                text=text, 
                command=command,
                width=40
            )
            btn.pack(pady=10, padx=20, fill='x')
    
    def setup_reports_tab(self):
        report_frame = ttk.Frame(self.reports_frame)
        report_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        ttk.Button(report_frame, text="📊 Show Final Report", command=self.show_final_report).pack(pady=10)
        
        self.report_text = scrolledtext.ScrolledText(
            report_frame, 
            height=25, 
            font=('Consolas', 10),
            bg='white'
        )
        self.report_text.pack(fill='both', expand=True)
    
    def log_to_gui(self, message):
        """Callback function for system to display in GUI"""
        def append():
            try:
                self.output_text.insert(tk.END, message + "\n")
                self.output_text.see(tk.END)
            except:
                pass
        
        if self.root:
            self.root.after(0, append)
    
    def start_system(self):
        if not self.running:
            self.system = DistributedBankSystem(gui_callback=self.log_to_gui)
            self.system.setup_infrastructure()
            self.system.start_system()
            
            self.running = True
            self.status_text.set("✅ System Running")
            self.start_btn.config(state='disabled')
            self.stop_btn.config(state='normal')
            
            self.refresh_accounts_list()
            self.log_to_gui("[SYSTEM] System started successfully")
    
    def stop_system(self):
        if self.running and self.system:
            def stop_thread():
                self.system.stop_system()
                self.root.after(0, self.on_system_stopped)
            
            threading.Thread(target=stop_thread, daemon=True).start()
    
    def on_system_stopped(self):
        self.running = False
        self.status_text.set("⏸️ System Stopped")
        self.start_btn.config(state='normal')
        self.stop_btn.config(state='disabled')
        self.refresh_accounts_list()
    
    def clear_output(self):
        self.output_text.delete(1.0, tk.END)
    
    def add_account(self):
        if not self.running or not self.system:
            messagebox.showerror("Error", "Please start the system first")
            return
            
        acc_id = self.acc_id_entry.get().strip()
        balance = self.acc_balance_entry.get().strip()
        branch = self.acc_branch_combo.get()
        
        if not acc_id or not balance:
            messagebox.showerror("Error", "Please enter account ID and balance")
            return
        
        try:
            balance = int(balance)
        except:
            messagebox.showerror("Error", "Balance must be a number")
            return
        
        self.system.accounts[acc_id] = Account(acc_id, balance, branch)
        self.log_to_gui(f"[SYSTEM] Account {acc_id} created with balance {balance} at {branch}")
        self.refresh_accounts_list()
        
        self.acc_id_entry.delete(0, tk.END)
        self.acc_balance_entry.delete(0, tk.END)
    
    def refresh_accounts_list(self):
        for item in self.accounts_tree.get_children():
            self.accounts_tree.delete(item)
        
        if self.running and self.system and hasattr(self.system, 'accounts'):
            for acc_id, acc in self.system.accounts.items():
                self.accounts_tree.insert('', tk.END, values=(acc_id, f"{acc.balance}$", acc.branch))
    
    def execute_transaction(self):
        if not self.running or not self.system:
            messagebox.showerror("Error", "Please start the system first")
            return
        
        tx_type = self.tx_type.get()
        
        if tx_type == "TRANSFER":
            from_acc = self.from_account_entry.get().strip()
            to_acc = self.to_account_entry.get().strip()
            
            try:
                amount = int(self.amount_entry.get().strip())
            except:
                messagebox.showerror("Error", "Amount must be a number")
                return
            
            if from_acc and to_acc and amount > 0:
                self.system.add_transaction(tx_type, from_account=from_acc, to_account=to_acc, amount=amount)
                self.log_to_gui(f"[USER] Transfer {amount}$ from {from_acc} to {to_acc}")
            else:
                messagebox.showerror("Error", "Please fill all fields correctly")
        
        elif tx_type in ["DEPOSIT", "WITHDRAW", "BALANCE"]:
            account = self.account_entry.get().strip()
            
            if tx_type in ["DEPOSIT", "WITHDRAW"]:
                try:
                    amount = int(self.amount_entry.get().strip())
                except:
                    messagebox.showerror("Error", "Amount must be a number")
                    return
                
                self.system.add_transaction(tx_type, account=account, amount=amount)
                self.log_to_gui(f"[USER] {tx_type} {amount}$ to/from {account}")
            else:
                self.system.add_transaction(tx_type, account=account)
                self.log_to_gui(f"[USER] Balance inquiry for {account}")
        
        else:
            self.system.add_transaction(tx_type)
            self.log_to_gui(f"[USER] Transaction {tx_type} added")
        

        self.root.after(2000, self.refresh_accounts_list)
    
    def run_basic_scenario(self):
        if not self.running or not self.system:
            self.start_system()
            self.root.after(500, self.run_basic_scenario)
            return
        
        self.log_to_gui("\n" + "="*50)
        self.log_to_gui("Running Basic Scenario")
        self.log_to_gui("="*50)
        

        self.system.add_transaction("BALANCE", account="ACC-A")
        self.system.add_transaction("DEPOSIT", account="ACC-B", amount=200)
        self.system.add_transaction("WITHDRAW", account="ACC-Joint", amount=500)
        self.system.add_transaction("TRANSFER", from_account="ACC-A", to_account="ACC-B", amount=300)
    
    def run_sjf_scenario(self):
        if not self.running or not self.system:
            self.start_system()
            self.root.after(500, self.run_sjf_scenario)
            return
        
        self.log_to_gui("\n" + "="*50)
        self.log_to_gui("Running SJF & Aging Scenario")
        self.log_to_gui("="*50)
        

        self.system.add_transaction("HEAVY_CALC")
        time.sleep(0.1)
        

        for i in range(5):
            self.system.add_transaction("BALANCE", account="ACC-A")
            time.sleep(0.1)
    
    def run_mutex_scenario(self):
        if not self.running or not self.system:
            self.start_system()
            self.root.after(500, self.run_mutex_scenario)
            return
        
        self.log_to_gui("\n" + "="*50)
        self.log_to_gui("Running Mutex Scenario (Simultaneous Withdrawal)")
        self.log_to_gui("="*50)
        

        self.system.add_transaction("WITHDRAW", account="ACC-Joint", amount=1000)
        time.sleep(0.1)
        self.system.add_transaction("WITHDRAW", account="ACC-Joint", amount=1000)
    
    def run_deadlock_scenario(self):
        if not self.running or not self.system:
            self.start_system()
            self.root.after(500, self.run_deadlock_scenario)
            return
        
        self.log_to_gui("\n" + "="*50)
        self.log_to_gui("Running Deadlock Scenario")
        self.log_to_gui("="*50)
        

        self.system.add_transaction("TRANSFER", from_account="ACC-A", to_account="ACC-B", amount=100)
        time.sleep(0.1)
        self.system.add_transaction("TRANSFER", from_account="ACC-B", to_account="ACC-A", amount=100)
    
    def run_comprehensive_scenario(self):
        if not self.running or not self.system:
            self.start_system()
            self.root.after(500, self.run_comprehensive_scenario)
            return
        
        self.log_to_gui("\n" + "="*50)
        self.log_to_gui("Running Comprehensive Scenario (Project Sample)")
        self.log_to_gui("="*50)
        

        transactions = [
            ("ANNUAL_PROFIT", None, 0, None, None),  # T1 Heavy (10s)
            ("MINI_STATEMENT", "ACC-A", 0, None, None),  # T2 (1s)
            ("FAST_CASH", "ACC-A", 50, None, None),  # T3 (2s)
            ("MOBILE_TOPUP", "ACC-B", 20, None, None),  # T4 (2s)
            ("UTILITY_BILL", "ACC-B", 100, None, None),  # T5 (3s)
            ("INTERNET_RECHARGE", "ACC-Joint", 30, None, None),  # T6 (3s)
            ("CARD_TO_CARD", "ACC-A", 200, None, None),  # T7 (4s)
            ("LOAN_INSTALLMENT", "ACC-B", 500, None, None),  # T8 (5s)
        ]
        
        for tx_type, account, amount, from_acc, to_acc in transactions:
            if tx_type == "TRANSFER":
                self.system.add_transaction(tx_type, from_account=from_acc, to_account=to_acc, amount=amount)
            elif account:
                self.system.add_transaction(tx_type, account=account, amount=amount)
            else:
                self.system.add_transaction(tx_type)
            time.sleep(0.2)  
        
        # Step 4: Mutex scenario
        time.sleep(2)
        self.log_to_gui("\n[STEP 4] Running Mutex Scenario")
        self.system.add_transaction("WITHDRAW", account="ACC-Joint", amount=1000)
        time.sleep(0.1)
        self.system.add_transaction("WITHDRAW", account="ACC-Joint", amount=1000)
        
        # Step 5: Deadlock scenario
        time.sleep(3)
        self.log_to_gui("\n[STEP 5] Running Deadlock Scenario")
        self.system.add_transaction("TRANSFER", from_account="ACC-A", to_account="ACC-B", amount=100)
        time.sleep(0.1)
        self.system.add_transaction("TRANSFER", from_account="ACC-B", to_account="ACC-A", amount=100)
    
    def show_final_report(self):
        if not self.running or not self.system:
            messagebox.showerror("Error", "System is not running")
            return
        
        self.report_text.delete(1.0, tk.END)
        
        report = f"""
{'='*60}
           DISTRIBUTED BANK SYSTEM - FINAL REPORT
{'='*60}

STATISTICS:
----------
✅ Successful Transactions: {self.system.successful_tx}
❌ Failed Transactions: {self.system.failed_tx}
🔄 Deadlocks Resolved: {self.system.deadlocks_resolved}
⚠️ Starvation Cases: {self.system.starvation_cases}
📊 Total Transactions: {self.system.transaction_count}

{'='*60}
FINAL ACCOUNT BALANCES:
{'-'*40}
"""
        
        for acc_id, acc in self.system.accounts.items():
            report += f"{acc_id:<15} : {acc.balance:>10,}$ (Branch: {acc.branch})\n"
        
        report += f"""
{'-'*40}
TOTAL SYSTEM BALANCE: {sum(acc.balance for acc in self.system.accounts.values()):,}$
{'='*60}
"""
        
        self.report_text.insert(1.0, report)


def main():
    root = tk.Tk()
    app = BankSystemGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()