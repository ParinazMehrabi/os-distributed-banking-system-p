#ifndef GUI_IPC_H
#define GUI_IPC_H

void gui_init();
void gui_send(const char* phase,
              const char* entity,
              const char* state,
              const char* info);

#endif
