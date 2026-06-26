#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define SECRET_KEY 'X'
#define AUTH_TOKEN "BRANCH_101_SECRET"

void encrypt_decrypt(char *data) {
    for (int i = 0; data[i] != '\0'; i++)
        data[i] ^= SECRET_KEY;
}

int checksum(const char *data) {
    int sum = 0;
    for (int i = 0; data[i] != '\0'; i++)
        sum += data[i];
    return sum;
}

int main() {
    int fd[2];
    pipe(fd);
    pid_t pid = fork();

    if (pid > 0) {
        close(fd[0]);

        char payload[] = "TRANSFER:1000:ACC101:ACC102";
        encrypt_decrypt(payload);

        int cs = checksum(payload);

        char message[200];
        sprintf(message, "%s|%s|%d", AUTH_TOKEN, payload, cs);

        printf("[PARENT] Encrypted & Authenticated Message:\n%s\n", message);

        write(fd[1], message, strlen(message) + 1);
        close(fd[1]);
    }
    else {
        close(fd[1]);

        char buffer[200];
        read(fd[0], buffer, sizeof(buffer));

        printf("\n[CHILD] Raw Message Received:\n%s\n", buffer);

        char recv_token[50], encrypted_data[100];
        int recv_checksum;

        sscanf(buffer, "%[^|]|%[^|]|%d",
               recv_token, encrypted_data, &recv_checksum);

        if (strcmp(recv_token, AUTH_TOKEN) != 0) {
            printf("[AUTH FAILED] Unauthorized sender!\n");
            return 0;
        }
        if (checksum(encrypted_data) != recv_checksum) {
            printf("[INTEGRITY FAILED] Data tampered!\n");
            return 0;
        }

        encrypt_decrypt(encrypted_data);
        printf("[CHILD] Decrypted Transaction:\n%s\n", encrypted_data);

        close(fd[0]);
    }

    return 0;
}
