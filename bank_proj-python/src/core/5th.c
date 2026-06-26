#include <stdio.h>
#include <stdbool.h>
#define P 5
#define R 3 
void calculateNeed(int need[P][R], int max[P][R], int allot[P][R]) {
    for (int i = 0; i < P; i++)
        for (int j = 0; j < R; j++)
            need[i][j] = max[i][j] - allot[i][j];
}

bool isSafe(int avail[], int max[][R], int allot[][R]) {
    int need[P][R];
    calculateNeed(need, max, allot);

    bool finish[P] = {0};
    int safeSeq[P];
    int work[R];

    for (int i = 0; i < R; i++) work[i] = avail[i];

    int count = 0;
    while (count < P) {
        bool found = false;
        for (int p = 0; p < P; p++) {
            if (finish[p] == 0) {
                int j;
                for (j = 0; j < R; j++)
                    if (need[p][j] > work[j])
                        break;

                if (j == R) { 
                    for (int k = 0; k < R; k++)
                        work[k] += allot[p][k];
                    
                    safeSeq[count++] = p;
                    finish[p] = 1;
                    found = true;
                }
            }
        }
        if (found == false) return false; 
    }
    printf("   -> Safe Seq Found: <");
    for (int i = 0; i < P; i++) printf(" P%d ", safeSeq[i]);
    printf(">\n");
    return true;
}
void requestResources(int p_id, int request[], int avail[], int max[][R], int allot[][R]) {
    int need[P][R];
    calculateNeed(need, max, allot);

    printf("\n[REQUEST] Process P%d requesting resources {%d, %d, %d}...\n",
           p_id, request[0], request[1], request[2]);

    for (int i = 0; i < R; i++) {
        if (request[i] > need[p_id][i]) {
            printf("[ERROR] Request exceeds maximum declared need.\n");
            return;
        }
    }
    for (int i = 0; i < R; i++) {
        if (request[i] > avail[i]) {
            printf("[DENIED] Not enough resources available instantly.\n");
            return;
        }
    }
    for (int i = 0; i < R; i++) {
        avail[i] -= request[i];
        allot[p_id][i] += request[i];
    }
    if (isSafe(avail, max, allot)) {
        printf("[GRANTED] System is in SAFE State. Request approved.\n");
    } else {
        printf("[DENIED] Request leads to UNSAFE State.\n");
        printf("          Rolling back allocation.\n");

        for (int i = 0; i < R; i++) {
            avail[i] += request[i];
            allot[p_id][i] -= request[i];
        }
    }
}


int main() {
    printf("BANKER'S ALGORITHM (AVOIDANCE)\n");

    int avail[R] = {3, 3, 2}; 

    int max[P][R] = {         
        {7, 5, 3}, // P0
        {3, 2, 2}, // P1
        {9, 0, 2}, // P2
        {2, 2, 2}, // P3
        {4, 3, 3}  // P4
    };

    int allot[P][R] = {     
        {0, 1, 0}, // P0
        {2, 0, 0}, // P1
        {3, 0, 2}, // P2
        {2, 1, 1}, // P3
        {0, 0, 2}  // P4
    };
    printf("\n[CHECK] Initial System State check:\n");
    isSafe(avail, max, allot);

    int req1[] = {1, 0, 2};
    requestResources(1, req1, avail, max, allot);

    int req2[] = {3, 3, 0}; 
    requestResources(4, req2, avail, max, allot);

    return 0;
}
