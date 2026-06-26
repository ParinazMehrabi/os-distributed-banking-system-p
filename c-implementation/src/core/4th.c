#include <stdio.h>
#include <pthread.h>
#include <unistd.h>


typedef enum {
    NEW,
    READY,
    RUNNING,
    BLOCKED,
    TERMINATED
} State;
typedef struct {
    int balance;
} Account;

Account joint_account = {1000};
pthread_mutex_t account_mutex;
void print_state(const char* name, State state) {
    const char* states[] = {
        "NEW", "READY", "RUNNING", "BLOCKED", "TERMINATED"
    };
    printf("[THREAD-%s] STATE -> %s\n", name, states[state]);
}

void* husband(void* arg) {
    print_state("HUSBAND", NEW);
    print_state("HUSBAND", READY);

    printf("[THREAD-HUSBAND] Requesting ACC-Joint (Balance: %d)\n",
           joint_account.balance);

    if (pthread_mutex_trylock(&account_mutex) != 0) {
        print_state("HUSBAND", BLOCKED);
        pthread_mutex_lock(&account_mutex);
    }

    print_state("HUSBAND", RUNNING);
    printf("[THREAD-HUSBAND] MUTEX LOCKED\n");

    if (joint_account.balance >= 1000) {
        sleep(1); 
        joint_account.balance -= 1000;
        printf("[THREAD-HUSBAND] Withdrawal successful. New Balance: %d\n",
               joint_account.balance);
    } else {
        printf("[THREAD-HUSBAND] ERROR: Insufficient Funds\n");
    }

    pthread_mutex_unlock(&account_mutex);
    printf("[THREAD-HUSBAND] MUTEX RELEASED\n");

    print_state("HUSBAND", TERMINATED);
    return NULL;
}
void* wife(void* arg) {
    print_state("WIFE", NEW);
    print_state("WIFE", READY);

    printf("[THREAD-WIFE] Requesting ACC-Joint (Balance: %d)\n",
           joint_account.balance);

    if (pthread_mutex_trylock(&account_mutex) != 0) {
        print_state("WIFE", BLOCKED);
        pthread_mutex_lock(&account_mutex);
    }

    print_state("WIFE", RUNNING);
    printf("[THREAD-WIFE] MUTEX LOCKED\n");

    if (joint_account.balance >= 1000) {
        sleep(1);
        joint_account.balance -= 1000;
        printf("[THREAD-WIFE] Withdrawal successful. New Balance: %d\n",
               joint_account.balance);
    } else {
        printf("[THREAD-WIFE] ERROR: Insufficient Funds. Transaction Aborted.\n");
    }
    pthread_mutex_unlock(&account_mutex);
    printf("[THREAD-WIFE] MUTEX RELEASED\n");

    print_state("WIFE", TERMINATED);
    return NULL;
}

int main() {
    pthread_t t1, t2;

    pthread_mutex_init(&account_mutex, NULL);

    printf("[SYSTEM] Joint Account Initialized. Balance = %d\n",
           joint_account.balance);

    pthread_create(&t1, NULL, husband, NULL);
    pthread_create(&t2, NULL, wife, NULL);

    pthread_join(t1, NULL);
    pthread_join(t2, NULL);

    printf("[SYSTEM] Final Balance: %d\n", joint_account.balance);

    pthread_mutex_destroy(&account_mutex);
    return 0;
}
