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
    int wait_time;
    int effective_bt; 
    ThreadState state;
    pthread_t tid;
} TCB;

#define MAX_THREADS 6
TCB tcbs[MAX_THREADS];
void* transaction_run(void* arg) {
    TCB* tcb = (TCB*) arg;

    tcb->state = RUNNING;
    printf("[Thread %d] State -> RUNNING (Burst=%d)\n", tcb->transaction_id, tcb->burst_time);

    sleep(tcb->burst_time);

    tcb->state = TERMINATED;
    printf("[Thread %d] State -> TERMINATED\n", tcb->transaction_id);
    pthread_exit(NULL);
}

void sort_ready_queue(TCB arr[], int n) {
    for (int i = 0; i < n - 1; i++) {
        for (int j = 0; j < n - i - 1; j++) {
            if (arr[j].effective_bt > arr[j + 1].effective_bt) {
                TCB temp = arr[j];
                arr[j] = arr[j + 1];
                arr[j + 1] = temp;
            }
        }
    }
}

void apply_aging(TCB arr[], int n) {
    for (int i = 0; i < n; i++) {
        if (arr[i].state == READY) {
            arr[i].wait_time += 1;
            arr[i].effective_bt = arr[i].burst_time - (arr[i].wait_time / 2); 
            if (arr[i].effective_bt < 1) arr[i].effective_bt = 1;
        }
    }
}

int main() {
    printf("====================================\n");
    printf("[SYSTEM]: Phase 3 - SJF + Aging Started\n");
    printf("====================================\n\n");

    int burst_values[MAX_THREADS] = {6, 3, 8, 2, 5, 10};
    for (int i = 0; i < MAX_THREADS; i++) {
        tcbs[i].transaction_id = i + 1;
        tcbs[i].target_account = i % 3;
        tcbs[i].amount = 300;
        tcbs[i].burst_time = burst_values[i];
        tcbs[i].wait_time = 0;
        tcbs[i].effective_bt = burst_values[i];
        tcbs[i].state = READY;

        printf("[Thread %d] Created (Burst=%d, State=READY)\n", i + 1, tcbs[i].burst_time);
    }

    sort_ready_queue(tcbs, MAX_THREADS);
    printf("\n[SCHEDULER]: Initial SJF Queue Sorted (Shortest job first)\n");
    for (int i = 0; i < MAX_THREADS; i++) {
        printf(" > T%d: Effective Burst=%d\n", tcbs[i].transaction_id, tcbs[i].effective_bt);
    }

    printf("\n[MONITOR]: Applying Dynamic Aging...\n");
    for (int cycle = 0; cycle < 3; cycle++) { 
        sleep(1);
        apply_aging(tcbs, MAX_THREADS);
        sort_ready_queue(tcbs, MAX_THREADS);
        printf("Cycle %d - Updated READY Queue:\n", cycle + 1);
        for (int i = 0; i < MAX_THREADS; i++) {
            printf(" > T%d: Burst=%d | Effective=%d | Wait=%d\n",
                   tcbs[i].transaction_id,
                   tcbs[i].burst_time,
                   tcbs[i].effective_bt,
                   tcbs[i].wait_time);
        }
    }

    printf("\n[SCHEDULER]: Dispatching Threads Based on SJF + Aging\n\n");
    for (int i = 0; i < MAX_THREADS; i++) {
        pthread_create(&tcbs[i].tid, NULL, transaction_run, &tcbs[i]);
        // pthread_join(tcbs[i].tid, NULL);
    }
    for(int i=0 ; i< MAX_THREADS; ++i)
    {
        pthread_join(tcbs[i].tid, NULL);
    }
    printf("[SYSTEM]: All Transactions Completed (SJF + Aging)\n");
  
    return 0;
}
