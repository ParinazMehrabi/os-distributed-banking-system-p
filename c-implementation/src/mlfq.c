#include <stdio.h>
#include <stdbool.h>

#define MAX_TASKS 20
#define AGING_THRESHOLD 8

typedef enum {
    NEW, READY, RUNNING, WAITING, TERMINATED
} State;

typedef struct {
    int id;
    int burst_time;
    int remaining_time;
    int queue_level;      
    int waiting_time;
    State state;
} Transaction;

typedef struct {
    int items[MAX_TASKS];
    int front, rear;
} Queue;

void initQueue(Queue *q) {
    q->front = q->rear = -1;
}

bool isEmpty(Queue *q) {
    return q->front == -1;
}

void enqueue(Queue *q, int val) {
    if (q->rear == MAX_TASKS - 1) return;
    if (q->front == -1) q->front = 0;
    q->items[++q->rear] = val;
}

int dequeue(Queue *q) {
    if (isEmpty(q)) return -1;
    int val = q->items[q->front++];
    if (q->front > q->rear)
        q->front = q->rear = -1;
    return val;
}
const char* stateName(State s) {
    switch (s) {
        case NEW: return "NEW";
        case READY: return "READY";
        case RUNNING: return "RUNNING";
        case WAITING: return "WAITING";
        case TERMINATED: return "TERMINATED";
    }
    return "";
}
void simulateMLFQ(Transaction t[], int n) {
    Queue Q0, Q1, Q2;
    initQueue(&Q0);
    initQueue(&Q1);
    initQueue(&Q2);

    int time = 0, completed = 0;

    printf("\n[MLFQ SCHEDULER STARTED]\n");

    for (int i = 0; i < n; i++) {
        t[i].state = READY;
        enqueue(&Q0, i);
        printf("[T%d] %s -> READY (Q0)\n", t[i].id, stateName(NEW));
    }

    while (completed < n) {
        Queue *currentQ = NULL;
        int quantum = 0;

        if (!isEmpty(&Q0)) {
            currentQ = &Q0;
            quantum = 2;
        } else if (!isEmpty(&Q1)) {
            currentQ = &Q1;
            quantum = 4;
        } else {
            currentQ = &Q2;
            quantum = 1000;
        }

        int idx = dequeue(currentQ);
        if (idx == -1) continue;

        Transaction *cur = &t[idx];
        cur->state = RUNNING;

        printf("\n[T%d] READY -> RUNNING | Q%d | Remaining=%d\n",
               cur->id, cur->queue_level, cur->remaining_time);

        int exec = (cur->remaining_time < quantum) ?
                    cur->remaining_time : quantum;

        time += exec;
        cur->remaining_time -= exec;
        for (int i = 0; i < n; i++) {
            if (t[i].state == READY) {
                t[i].waiting_time += exec;
                if (t[i].waiting_time >= AGING_THRESHOLD &&
                    t[i].queue_level > 0) {

                    printf("[AGING] T%d promoted to Q0\n", t[i].id);
                    t[i].queue_level = 0;
                    t[i].waiting_time = 0;
                    enqueue(&Q0, i);
                }
            }
        }

        if (cur->remaining_time == 0) {
            cur->state = TERMINATED;
            completed++;
            printf("[T%d] RUNNING -> TERMINATED at time %d\n",
                   cur->id, time);
        } else {
            cur->state = READY;
            cur->queue_level++;

            if (cur->queue_level == 1)
                enqueue(&Q1, idx);
            else
                enqueue(&Q2, idx);

            printf("[T%d] TIMEOUT -> READY (Demoted to Q%d)\n",
                   cur->id, cur->queue_level);
        }
    }

    printf("\n[MLFQ FINISHED] Total Time = %d\n", time);
}
int main() {
    Transaction tasks[] = {
        {1, 2, 2, 0, 0, NEW},
        {2, 10, 10, 0, 0, NEW},
        {3, 4, 4, 0, 0, NEW},
        {4, 1, 1, 0, 0, NEW}
    };

    simulateMLFQ(tasks, 4);
    return 0;
}
