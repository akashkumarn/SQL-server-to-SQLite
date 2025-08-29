# SQL Server ➝ SQLite Migrator

A simple Python-based tool (with GUI) that **migrates a SQL Server database into a SQLite file**.
It preserves tables, primary keys, foreign keys, and unique constraints.

Built with:

* **Python**
* **Tkinter** (GUI frontend)
* **pyodbc** (SQL Server connection)
* **sqlite3** (SQLite handling)

---

## ✨ Features

* 🗄️ Migrate all SQL Server tables into a single `output.db` SQLite file
* 🔑 Preserves **primary keys, unique constraints, and foreign keys**
* 🎨 Simple **Tkinter GUI** → no need to use command line
* ⚡ Converts SQL Server data types into SQLite-friendly equivalents

---

## 📦 Installation

1. Clone this repo:

   ```bash
   git clone https://github.com/YOUR-USERNAME/sqlserver-to-sqlite.git
   cd sqlserver-to-sqlite
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Run the GUI:

   ```bash
   python app.py
   ```

---

## 🖥️ Usage

1. Launch the app:

   ```bash
   python app.py
   ```

2. Enter your **SQL Server connection details**:

   * Server
   * Database
   * Username
   * Password

3. Click **Run Migration**.

4. Wait for the process → You’ll see progress logs in the window.

5. When done, you’ll find your SQLite file as **`output.db`** in the project folder.

---

## ⚠️ Notes

* Requires **Python 3.8+**
* Works with **SQL Server authentication** (not Windows Auth by default).
* For large databases, migration may take some time.

---

## 🚀 Future Improvements

* Add **progress bar** to the GUI
* Add option to select **which tables** to migrate
* Export as **.exe** (so users don’t need Python installed)

---

## 📜 License

This project is licensed under the [MIT License](LICENSE).
You’re free to use and modify it.
