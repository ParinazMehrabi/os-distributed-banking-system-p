#include "gui_ipc.h"
#include <stdio.h>

void gui_init() {
    printf("[GUI]: GUI disabled (Phase 1 Console Mode)\n");
}

void gui_send(const char* phase,
              const char* entity,
              const char* state,
              const char* info) {

    printf("[GUI][%s][%s][%s]: %s\n",
           phase, entity, state, info);
}
