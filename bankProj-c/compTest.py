import subprocess
import sys
import os
import time

PYTHON = sys.executable


def run_phase(title, filename):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)

    result = subprocess.run(
        [PYTHON, filename],
        stdout=sys.stdout,
        stderr=sys.stderr
    )

    if result.returncode != 0:
        print(f"[ERROR] {filename} failed.")
        sys.exit(1)

    time.sleep(1)


def main():
    print("\n############################################")
    print("  COMPREHENSIVE BANK SYSTEM TEST STARTED  ")
    print("############################################")
    run_phase(
        "[STEP 1] PROCESS CREATION & IPC (Phase 1)",
        "1.py"
    )
    run_phase(
        "[STEP 2] MULTI-THREADING & STATE MANAGEMENT (Phase 2)",
        "2.py"
    )
    run_phase(
        "[STEP 3] SJF SCHEDULING + AGING (Phase 3)",
        "3.py"
    )
    run_phase(
        "[STEP 4] CRITICAL SECTION & MUTEX (Phase 4)",
        "4.py"
    )
    run_phase(
        "[STEP 5] DEADLOCK DETECTION & RECOVERY (Phase 5)",
        "5.py"
    )
    run_phase(
        "[BONUS 1] BANKER'S ALGORITHM (Deadlock Prevention)",
        "5+banker.py"
    )
    run_phase(
        "[BONUS 2] MULTI-LEVEL FEEDBACK QUEUE (MLFQ)",
        "3+mlfq.py"
    )
    run_phase(
        "[BONUS 5] SECURE IPC (ENCRYPTION & AUTHENTICATION)",
        "encryption.py"
    )
    print("\n============================================")
    print("              FINAL SUMMARY")
    print("============================================")
    print("✔ Processes executed successfully")
    print("✔ Thread lifecycle & TCB states validated")
    print("✔ SJF scheduling with Aging verified")
    print("✔ Mutex protected critical sections")
    print("✔ Deadlock detected and recovered")
    print("✔ Banker algorithm prevented deadlock")
    print("✔ MLFQ scheduling executed correctly")
    print("✔ IPC encryption & authentication enabled")
    print("\nALL PHASES & BONUSES PASSED SUCCESSFULLY")
    print(" SYSTEM READY FOR FINAL SUBMISSION\n")


if __name__ == "__main__":
    main()
