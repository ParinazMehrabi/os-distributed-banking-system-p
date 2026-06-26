#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>

typedef enum {
    NEW,
    READY,
    RUNNING,
    WAITING,
    TERMINATED
} ThreadState;


typedef struct {
    int id;
    int balance;
} Account;

Account shared_accounts[3] = {
    {101, 5000},
    {102, 3000},
    {103, 1000}
};

typedef struct {
    int transaction_id;
    int target_account;
    int amount;
    int burst_time;
    ThreadState state;
    pthread_t tid;
} TCB;

#define MAX_THREADS 3
TCB* ready_queue[MAX_THREADS];
int ready_count = 0;

const char* state_to_string(ThreadState s) {
    switch (s) {
        case NEW: return "NEW";
        case READY: return "READY";
        case RUNNING: return "RUNNING";
        case WAITING: return "WAITING";
        case TERMINATED: return "TERMINATED";
        default: return "UNKNOWN";
    }
}

void* transaction_run(void* arg) {
    TCB* tcb = (TCB*) arg;

    tcb->state = RUNNING;
    printf("[Thread %d] State -> RUNNING (Account %d)\n",
           tcb->transaction_id,
           shared_accounts[tcb->target_account].id);

    int local_amount = tcb->amount;

    tcb->state = WAITING;
    printf("[Thread %d] State -> WAITING (Simulated resource wait)\n",
           tcb->transaction_id);
    sleep(1);

    tcb->state = RUNNING;
    printf("[Thread %d] State -> RUNNING (Processing transaction)\n",
           tcb->transaction_id);

    printf("[Thread %d] Reading balance: %d\n",
           tcb->transaction_id,
           shared_accounts[tcb->target_account].balance);

    if (shared_accounts[tcb->target_account].balance >= local_amount) {
        shared_accounts[tcb->target_account].balance -= local_amount;
        printf("[Thread %d] Withdrawal %d successful. New balance: %d\n",
               tcb->transaction_id,
               local_amount,
               shared_accounts[tcb->target_account].balance);
    } else {
        printf("[Thread %d] Withdrawal failed (Insufficient funds)\n",
               tcb->transaction_id);
    }

    sleep(tcb->burst_time);
    tcb->state = TERMINATED;
    printf("[Thread %d] State -> TERMINATED\n", tcb->transaction_id);

    pthread_exit(NULL);
}

int main() {
    TCB tcbs[MAX_THREADS];
    printf("[SYSTEM]: Phase 2 - Threading Started\n");
    printf("[PROCESS]: Bank Branch Process Running\n");
  

    for (int i = 0; i < MAX_THREADS; i++) {
        tcbs[i].transaction_id = i + 1;
        tcbs[i].target_account = i;
        tcbs[i].amount = 500;
        tcbs[i].burst_time = i + 1;
        tcbs[i].state = NEW;

        printf("[Thread %d] Created | State: NEW\n",
               tcbs[i].transaction_id);

        tcbs[i].state = READY;
        ready_queue[ready_count++] = &tcbs[i];

        printf("[Thread %d] Moved to READY Queue\n",
               tcbs[i].transaction_id);
    }

    printf("\n[SCHEDULER]: Dispatching READY threads...\n\n");

    for (int i = 0; i < ready_count; i++) {
        pthread_create(&ready_queue[i]->tid,
                       NULL,
                       transaction_run,
                       ready_queue[i]);
    }

    for (int i = 0; i < ready_count; i++) {
        pthread_join(ready_queue[i]->tid, NULL);
    }
    
    printf("[SYSTEM]: All Transactions Completed\n");
    return 0;
}
