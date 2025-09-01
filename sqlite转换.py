import sqlite3
import csv
import os

def export_tables_to_csv(sqlite_path, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    con = sqlite3.connect(sqlite_path)
    cur = con.cursor()

    # 只选普通表：type='table' 且 sql 不包含 'VIRTUAL'
    cur.execute("""
        SELECT name 
          FROM sqlite_master 
         WHERE type='table'
           AND sql NOT LIKE 'CREATE VIRTUAL%';
    """)
    tables = [r[0] for r in cur.fetchall()]

    for tbl in tables:
        try:
            cur.execute(f"SELECT * FROM \"{tbl}\";")
        except sqlite3.OperationalError as e:
            print(f"跳过表 {tbl}：{e}")
            continue

        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

        csv_file = os.path.join(out_dir, f"{tbl}.csv")
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            writer.writerows(rows)

        print(f"✔ 已导出表 {tbl} 到 {csv_file}")

    con.close()

    con.close()
if __name__ == "__main__":
    sqlite_db  = r"D:\华东师大\实践考核\毕业论文相关\实验分析\Dataset\188-million-us-wildfires\FPA_FOD_20170508.sqlite"
    output_directory  = r"D:\华东师大\实践考核\毕业论文相关\实验分析\Dataset\188-million-us-wildfires\FPA_FOD_20170508.sql"
    export_tables_to_csv(sqlite_db, output_directory)
