import tkinter as tk
from tkinter import messagebox
import threading


from sql_to_sqlite import run_migration

def start_migration():
    server = entry_server.get()
    database = entry_database.get()
    username = entry_username.get()
    password = entry_password.get()

    if not (server and database and username and password):
        messagebox.showerror("Error", "All fields are required!")
        return

    
    def task():
        try:
            run_migration(server, database, username, password)
            messagebox.showinfo("Success", "Migration complete! ✅\nOutput saved as output.db")
        except Exception as e:
            messagebox.showerror("Error", f"Migration failed: {e}")

    threading.Thread(target=task).start()


root = tk.Tk()
root.title("SQL Server ➝ SQLite Migrator")

tk.Label(root, text="Server:").grid(row=0, column=0, sticky="e")
tk.Label(root, text="Database:").grid(row=1, column=0, sticky="e")
tk.Label(root, text="Username:").grid(row=2, column=0, sticky="e")
tk.Label(root, text="Password:").grid(row=3, column=0, sticky="e")

entry_server = tk.Entry(root, width=30)
entry_database = tk.Entry(root, width=30)
entry_username = tk.Entry(root, width=30)
entry_password = tk.Entry(root, width=30, show="*")

entry_server.grid(row=0, column=1, padx=5, pady=5)
entry_database.grid(row=1, column=1, padx=5, pady=5)
entry_username.grid(row=2, column=1, padx=5, pady=5)
entry_password.grid(row=3, column=1, padx=5, pady=5)

btn = tk.Button(root, text="Run Migration", command=start_migration)
btn.grid(row=4, columnspan=2, pady=10)

root.mainloop()
