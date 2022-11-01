import tkinter as tk
from BitqueryClient import BitqueryClient
from threading import Thread
API_KEY = 'BQYDRmZAqxM7A1bfThsOS0HoA4zUXya1'

client = BitqueryClient(API_KEY)


def start():
    nP = int(e1.get())
    nT = int(e2.get())
    nL = int(e3.get())
    nB = int(e4.get())
    button['state'] = 'disabled'

    df = client.get_df(days=nP, limit=nT, last_tx_time=nL, n_visible_rows=nB)
    df.to_excel('top profit.xlsx', index=False)
    button['state'] = 'normal'

def start_thread():
    t = Thread(target=start)
    t.start()

master = tk.Tk()
tk.Label(master, text="nP").grid(row=0)
tk.Label(master, text="nT").grid(row=1)
tk.Label(master, text="nL").grid(row=2)
tk.Label(master, text="nB").grid(row=3)

e1 = tk.Entry(master)
e2 = tk.Entry(master)
e3 = tk.Entry(master)
e4 = tk.Entry(master)

e1.grid(row=0, column=1)
e2.grid(row=1, column=1)
e3.grid(row=2, column=1)
e4.grid(row=3, column=1)

button = tk.Button(master, text='Start', width=20, command=start_thread)
button.grid(row=4, columnspan=2)
master.mainloop()
