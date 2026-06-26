#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include "gui_ipc.h"

struct Account {
    char name[30];
    int balance;
};

void showAccounts(struct Account accs[], int n, const char* branchName) {
    printf("\n---- %s Accounts ----\n", branchName);
    for (int i = 0; i < n; i++) {
        printf("%s: %d\n", accs[i].name, accs[i].balance);
    }
    printf("-----------------------\n");
}

int main() {
    gui_init();

    int pipe_fd[2];
    pid_t pid;

    if (pipe(pipe_fd) == -1) {
        perror("Pipe creation failed");
        exit(1);
    }

    printf("[SYSTEM]: OS Kernel Initialized\n");
    gui_send("PHASE1", "SYSTEM", "INIT", "OS Kernel Initialized");

    pid = fork();

    if (pid < 0) {
        perror("Fork failed");
        exit(1);
    }
    if (pid == 0) {

        close(pipe_fd[0]);  

        struct Account digitalDB[3] = {
            {"Arman", 2500},
            {"Sara", 4200},
            {"CompanyX", 7200}
        };

        printf("[PROCESS %d]: Digital Branch Started\n", getpid());
        showAccounts(digitalDB, 3, "Digital Branch");

        gui_send("PHASE1", "DigitalBranch",
                 "PROCESS_STARTED",
                 "Local database initialized");

        char message[] = "Transfer 500 from Arman to CompanyX";
        write(pipe_fd[1], message, strlen(message) + 1);

        printf("[PROCESS %d]: Transaction Request Sent to Central Branch\n", getpid());
        gui_send("PHASE1", "IPC",
                 "PIPE_SEND",
                 message);

        close(pipe_fd[1]);
        exit(0);
    }
    else {

        close(pipe_fd[1]);   

        struct Account centralDB[3] = {
            {"Arman", 8000},
            {"Sara", 3500},
            {"CompanyX", 9200}
        };

        printf("[PROCESS %d]: Central Branch Started\n", getpid());
        showAccounts(centralDB, 3, "Central Branch");

        gui_send("PHASE1", "CentralBranch",
                 "PROCESS_STARTED",
                 "Waiting for incoming requests");

        char buffer[100];
        read(pipe_fd[0], buffer, sizeof(buffer));

        printf("[PROCESS %d]: Received Message: '%s'\n", getpid(), buffer);
        gui_send("PHASE1", "IPC",
                 "PIPE_RECEIVE",
                 buffer);

        printf("[PROCESS %d]: Transaction Verified and Logged.\n", getpid());
        gui_send("PHASE1", "CentralBranch",
                 "TRANSACTION_VERIFIED",
                 "Request logged successfully");

        close(pipe_fd[0]);
    }

    return 0;
}
