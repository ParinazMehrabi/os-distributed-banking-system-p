import socket
import threading
import tkinter as tk

root = tk.Tk()
root.title("OS Banking System Monitor")

log = tk.Text(root, width=80, height=25)
log.pack()

def listen():
    s = socket.socket()
    s.connect(("localhost", 9090))

    while True:
        msg = s.recv(256).decode()
        if not msg:
            break
        root.after(0, update_log, msg)

def update_log(msg):
    log.insert(tk.END, msg)
    log.see(tk.END)

threading.Thread(target=listen, daemon=True).start()
root.mainloop()
