import paramiko
import time
import sqlparse
import concurrent.futures
import configparser
from pathlib import Path

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

def execute_sql_in_persistent_psql_session(ip, port, username, password, sql_file_path, psql_command):
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

    # ——————— 指定全局配置文件 ———————
    # 该模块将读取server_config.conf配置文件，并获取相关参数
    config = configparser.ConfigParser()
    config_path = Path(__file__).parent / 'server_config.conf'  # 获取配置文件的绝对路径

    # ———————配置文件错误检查———————
    # 检查配置文件是否存在
    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    try:
        config.read(config_path, encoding='utf-8')
    except Exception as e:
        raise RuntimeError(f"读取配置文件失败: {str(e)}")
    # 验证配置文件内容
    if not config.sections():
        raise ValueError(f"配置文件内容为空或无法解析: {config_path}")

    # 从配置文件加载服务器列表
    servers = []
    for section in config.sections():
        if section.startswith('Server'):  # 匹配所有以'Server'开头的配置节
            try:
                server = {
                    "ip": config.get(section, 'ip'),
                    "port": config.getint(section, 'port'),  # 整数类型
                    "username": config.get(section, 'username'),
                    "password": config.get(section, 'password'),
                    "sql_file_path": config.get(section, 'sql_file_path'),  # 新增：SQL文件路径
                    "psql_command": config.get(section, 'psql_command'),  # 新增：psql命令
                    "psql_close_command": config.get(section, 'psql_close_command')  # 新增：关闭命令
                }
                servers.append(server)
            except configparser.NoOptionError as e:
                print(f"配置文件错误：{section} 缺少必要配置项: {e}")
                continue

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

