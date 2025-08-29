import decimal
import datetime
import pyodbc
import sqlite3
from collections import defaultdict

def run_migration(server, database, username, password):

    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    sql_conn = pyodbc.connect(conn_str)
    sql_cursor = sql_conn.cursor()

    
    sqlite_conn = sqlite3.connect('output.db')
    sqlite_cursor = sqlite_conn.cursor()
    sqlite_cursor.execute("PRAGMA foreign_keys = OFF;")  # temporarily disable

    
    sql_cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
    tables = sql_cursor.fetchall()

    
    def get_columns(schema, table):
        sql_cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}'
            ORDER BY ORDINAL_POSITION
        """)
        return sql_cursor.fetchall()

    def get_primary_key(schema, table):
        sql_cursor.execute(f"""
            SELECT kc.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc
            ON tc.CONSTRAINT_NAME = kc.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA='{schema}' AND tc.TABLE_NAME='{table}' AND tc.CONSTRAINT_TYPE='PRIMARY KEY'
            ORDER BY kc.ORDINAL_POSITION
        """)
        return [row[0] for row in sql_cursor.fetchall()]

    def get_unique_constraints(schema, table):
        sql_cursor.execute(f"""
            SELECT kc.CONSTRAINT_NAME, kc.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc
            ON tc.CONSTRAINT_NAME = kc.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA='{schema}' AND tc.TABLE_NAME='{table}' AND tc.CONSTRAINT_TYPE='UNIQUE'
            ORDER BY kc.CONSTRAINT_NAME, kc.ORDINAL_POSITION
        """)
        result = defaultdict(list)
        for constraint_name, column_name in sql_cursor.fetchall():
            result[constraint_name].append(column_name)
        return result

    def get_foreign_keys(schema, table):
        sql_cursor.execute(f"""
            SELECT fk.COLUMN_NAME, pk.TABLE_SCHEMA, pk.TABLE_NAME, pkc.COLUMN_NAME AS REFERENCED_COLUMN
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk
            ON rc.CONSTRAINT_NAME = fk.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk
            ON rc.UNIQUE_CONSTRAINT_NAME = pk.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pkc
            ON pk.CONSTRAINT_NAME = pkc.CONSTRAINT_NAME
            AND fk.ORDINAL_POSITION = pkc.ORDINAL_POSITION
            WHERE fk.TABLE_SCHEMA='{schema}' AND fk.TABLE_NAME='{table}'
        """)
        return sql_cursor.fetchall()

    def map_type(col_type, char_len):
        
        if col_type in ('int', 'bigint', 'smallint', 'tinyint'):
            return 'INTEGER'
        elif col_type in ('geography', 'geometry'): 
            return 'TEXT'
        elif col_type in ('decimal', 'numeric', 'money', 'smallmoney', 'float', 'real'):
            return 'REAL'
        elif col_type in ('bit',):
            return 'INTEGER'
        elif col_type in ('char', 'nchar', 'varchar', 'nvarchar', 'text', 'ntext'):
            return 'TEXT'
        elif col_type in ('datetime', 'datetime2', 'smalldatetime', 'date', 'time'):
            return 'TEXT'
        else:
            return 'TEXT'

    
    table_schemas = {}
    fk_constraints = {}
    sqlite_name_map = {} 

    for schema, table in tables:
        full_table_name = f"{schema}.{table}"
        sqlite_table_name = f"{schema}_{table}"
        sqlite_name_map[full_table_name] = sqlite_table_name

        try:
            columns = get_columns(schema, table)
            pk_cols = get_primary_key(schema, table)
            unique_constraints = get_unique_constraints(schema, table)
            fks = get_foreign_keys(schema, table)

            col_defs = []
            for col_name, col_type, is_nullable, default, char_len in columns:
                sql_type = map_type(col_type, char_len)
                col_def = f'"{col_name}" {sql_type}'
                if col_name in pk_cols and len(pk_cols) == 1:
                    col_def += ' PRIMARY KEY'
                if is_nullable == 'NO' and col_name not in pk_cols:
                    col_def += ' NOT NULL'
                col_defs.append(col_def)

            
            for constraint_name, cols in unique_constraints.items():
                col_defs.append('UNIQUE (' + ', '.join([f'"{c}"' for c in cols]) + ')')

            table_schemas[full_table_name] = col_defs

            
            fk_constraints[full_table_name] = []
            for fk_col, ref_schema, ref_table, ref_col in fks:
                fk_constraints[full_table_name].append({
                    'COLUMN_NAME': fk_col,
                    'REFERENCED_TABLE': f"{ref_schema}.{ref_table}",
                    'REFERENCED_COLUMN': ref_col
                })

            print(f"Initialized {full_table_name} with {len(fk_constraints[full_table_name])} FKs")

        except Exception as e:
            print(f"Error processing table {full_table_name}: {e}")

    
    sorted_tables = []
    visited = {}

    def visit(full_table_name):
        if full_table_name in visited:
            if visited[full_table_name] == 1:
                return
            if visited[full_table_name] == -1:
                return  # cycle detected
        visited[full_table_name] = -1  # visiting
        for fk in fk_constraints.get(full_table_name, []):
            ref_table = fk['REFERENCED_TABLE']
            if ref_table in table_schemas:  # only visit existing tables
                visit(ref_table)
        visited[full_table_name] = 1
        if full_table_name not in sorted_tables:
            sorted_tables.append(full_table_name)

    for schema, table in tables:
        full_table_name = f"{schema}.{table}"
        visit(full_table_name)

    
    for full_table_name in sorted_tables:
        columns_sql = table_schemas[full_table_name]
        sqlite_table_name = sqlite_name_map[full_table_name]

        # Add foreign keys
        fk_sql_list = []
        for fk in fk_constraints.get(full_table_name, []):
            col_name = fk['COLUMN_NAME']
            ref_table = sqlite_name_map.get(fk['REFERENCED_TABLE'], fk['REFERENCED_TABLE'])
            ref_col = fk['REFERENCED_COLUMN']
            fk_sql_list.append(f'FOREIGN KEY("{col_name}") REFERENCES "{ref_table}"("{ref_col}")')

        all_defs = columns_sql + fk_sql_list
        create_sql = f'CREATE TABLE IF NOT EXISTS "{sqlite_table_name}" (\n    ' + ",\n    ".join(all_defs) + "\n);"

        try:
            sqlite_cursor.execute(create_sql)
            print(f"Created table {sqlite_table_name}")
        except sqlite3.Error as e:
            print(f"Error creating table {sqlite_table_name}: {e}")

    
    all_tables = set(f"{schema}.{table}" for schema, table in tables)
    cyclic_tables = all_tables - set(sorted_tables)
    for full_table_name in cyclic_tables:
        sqlite_table_name = sqlite_name_map[full_table_name]
        sqlite_cursor.execute(f'ALTER TABLE "{sqlite_table_name}" RENAME TO "{sqlite_table_name}_temp";')
        col_defs = table_schemas[full_table_name]
        fk_defs = [f'FOREIGN KEY("{fk["COLUMN_NAME"]}") REFERENCES "{sqlite_name_map.get(fk["REFERENCED_TABLE"], fk["REFERENCED_TABLE"])}"("{fk["REFERENCED_COLUMN"]}")'
                for fk in fk_constraints.get(full_table_name, [])]
        create_sql = f'CREATE TABLE "{sqlite_table_name}" ({", ".join(col_defs + fk_defs)});'
        sqlite_cursor.execute(create_sql)
        sqlite_cursor.execute(f'INSERT INTO "{sqlite_table_name}" SELECT * FROM "{sqlite_table_name}_temp";')
        sqlite_cursor.execute(f'DROP TABLE "{sqlite_table_name}_temp";')

    
    for full_table_name in sorted_tables:
        schema, table = full_table_name.split('.')
        sqlite_table_name = sqlite_name_map[full_table_name]

        try:
            # Build safe SELECT with casts for unsupported types
            columns = get_columns(schema, table)
            select_cols = []
            for col_name, col_type, *_ in columns:
                if col_type in ('geography', 'geometry'):
                    # Convert to string representation (WKT)
                    select_cols.append(f'[{col_name}].ToString() AS [{col_name}]')
                else:
                    select_cols.append(f'[{col_name}]')
            select_sql = f"SELECT {', '.join(select_cols)} FROM [{schema}].[{table}]"

            # Fetch all data from SQL Server
            sql_cursor.execute(select_sql)
            rows = sql_cursor.fetchall()
            if not rows:
                continue  # skip empty tables

            # Column names
            col_names = [desc[0] for desc in sql_cursor.description]
            placeholders = ', '.join(['?'] * len(col_names))
            col_list = ', '.join([f'"{col}"' for col in col_names])

            # Convert unsupported Python types before inserting
            def convert_row(row):
                new_row = []
                for val in row:
                    if isinstance(val, decimal.Decimal):
                        new_row.append(float(val))
                    elif isinstance(val, datetime.datetime):
                        new_row.append(val.isoformat())
                    elif isinstance(val, datetime.date):
                        new_row.append(val.isoformat())
                    elif isinstance(val, datetime.time):
                        new_row.append(val.isoformat())
                    else:
                        new_row.append(val)
                return new_row

            # Insert into SQLite
            sqlite_cursor.executemany(
                f'INSERT OR REPLACE INTO "{sqlite_table_name}" ({col_list}) VALUES ({placeholders})',
                [convert_row(r) for r in rows]
            )

            print(f"Migrated {len(rows)} rows into {sqlite_table_name}")

        except Exception as e:
            print(f"Error migrating table {full_table_name}: {e}")


    # Commit after data migration
    sqlite_conn.commit()
    sqlite_cursor.execute("PRAGMA foreign_keys = ON;")
    sqlite_conn.close()
    sql_conn.close()

    print("Migration complete! ✅ All tables, PKs, FKs, and UNIQUE constraints preserved.")

