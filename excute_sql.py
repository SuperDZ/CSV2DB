import paramiko
import time
import sqlparse
import concurrent.futures

def execute_sql_fast_no_output(ip, port, username, password, sql_file_path, psql_command, psql_close_command, repeat=20):
    print(f"\n>>> 正在连接 {ip} ...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, port=port, username=username, password=password)
    print("ssh连接成功")

    # 读取 SQL 脚本并按语句切分
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    print("sql文件读取成功")

    statements = sqlparse.split(sql_content)

    # 启动 psql 会话
    shell = ssh.invoke_shell()
    shell.send(f"{psql_command}\n")
    time.sleep(1)  # 等待 psql 启动

    # 快速执行，每次循环 1 轮 SQL
    for round_num in range(1, repeat + 1):
        print(f">>> 正在执行第 {round_num} 次脚本 ...")
        for statement in statements:
            clean_sql = statement.strip()
            if not clean_sql:
                continue
            shell.send(clean_sql + ";\n")
            time.sleep(2)  # 简短等待，保证语句送入

    # 退出 psql
    shell.send(psql_close_command+"\n")
    time.sleep(1)

    shell.close()
    ssh.close()
    print(">>> 所有执行完成，连接已关闭。")

def execute_sql_in_persistent_psql_session(ip, port, username, password, sql_file_path, psql_command, repeat=50):
    print(f"\n>>> 正在连接 {ip} ...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, port=port, username=username, password=password)
    print("ssh连接成功")

    # 读取 SQL 脚本并按语句切分
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    print("sql文件读取成功")

    statements = sqlparse.split(sql_content)

    # 打开交互式 shell，并启动 psql 会话
    shell = ssh.invoke_shell()
    shell.send(f"{psql_command}\n")
    time.sleep(1)  # 等待 psql 启动

    # 读取启动 psql 的初始提示信息
    if shell.recv_ready():
        print(shell.recv(4096).decode())

    for i, statement in enumerate(statements, start=1):
        clean_sql = statement.strip()
        if not clean_sql:
            continue
        # 输入 SQL（每句后加分号防止粘连）
        print(f">>> [第 {i} 条 SQL] 正在发送: {clean_sql[:80]}{'...' if len(clean_sql) > 80 else ''}")
        shell.send(clean_sql + ";\n")
        time.sleep(5)  # 等待语句执行完毕（可根据语句复杂度调整）
        while shell.recv_ready():
            output = shell.recv(4096).decode()
            print(output.strip())

    # 退出 psql
    shell.send("\\q\n")
    time.sleep(1)
    if shell.recv_ready():
        print(shell.recv(4096).decode())
    shell.send("\\q\n")
    shell.close()
    ssh.close()




if __name__ == "__main__":
    servers = [
        {
            "ip": "192.168.31.20",
            "port": 22,
            "username": "postgresql",
            "password": "md5654852",
            "sql_file_path": r"C:\Users\MZJ-Y9000P\Desktop\fsdownload\airlines_flights_data_modify_20.sql",
            "psql_command": "psql -d benchmarksql -p 5432",
            "psql_close_command": "pg_ctl -D /app/pgdata1 -l logfile stop"
        },
        {
            "ip": "192.168.31.10",
            "port": 22,
            "username": "postgresql",
            "password": "md5654852",
            "sql_file_path": r"C:\Users\MZJ-Y9000P\Desktop\fsdownload\airlines_flights_data_modify_10.sql",
            "psql_command": "/app/postgresql-15.10/opt/pgsql/bin/psql -d benchmarksql -p 5432",
            "psql_close_command": "pg_ctl -D /app/pgdata3 -l logfile stop"
        }
    ]

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # Submit both tasks at once
        futures = [
            executor.submit(
                execute_sql_fast_no_output,
                srv["ip"], srv["port"], srv["username"], srv["password"],
                srv["sql_file_path"], srv["psql_command"], srv["psql_close_command"]
            )
            for srv in servers
        ]
        # Optionally wait for both to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # will re‑raise any exceptions
            except Exception as e:
                print("Task raised an exception:", e)
    #
    # #第一台服务器
    # execute_sql_fast_no_output(
    #     ip="192.168.31.20",
    #     port=22,
    #     username="postgresql",
    #     password="md5654852",
    #     sql_file_path=r"C:\Users\MZJ-Y9000P\Desktop\fsdownload\test_sql_before.sql",
    #     psql_command="psql -d benchmarksql -p 5432"
    # )
    #
    # # # 第二台服务器
    # execute_sql_fast_no_output(
    #     ip="192.168.31.10",
    #     port=22,
    #     username="postgresql",
    #     password="md5654852",
    #     sql_file_path=r"C:\Users\MZJ-Y9000P\Desktop\fsdownload\test-after.sql",
    #     psql_command="/app/postgresql-15.10/opt/pgsql/bin/psql -d benchmarksql -p 5432"
    # )
