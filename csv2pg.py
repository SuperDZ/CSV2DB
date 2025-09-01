import paramiko
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import re

#2025.8.30 v5.0版本(目前是pg版本)
#支持将指定目录下的.csv数据集批量导入到pg数据库中，自动检测数据库是否存在，不存在则直接创建，文件夹需要小写命名
#不同数据集需要修改的地方：
#1. 数据集的路径
#2. 数据集的文件名
#3. 连接的服务器的ip地址、端口、用户名、密码、psql路径、pg端口
#code by MZJ

#将csv格式的文件转换成sql导入指定ip的服务器上的pg库中
#
# ——————— 全局配置 ———————
LOCAL_CSV_DIR = Path(r"在这里入本地csv文件夹目录，需要替换为C:XXX\XXXX\XXXX")

# 数据库名格式化处理
def sanitize_db_name(name: str) -> str:
    # 原始：空格 -> 下划线
    name = name.replace(' ', '_')
    # 非 [a-zA-Z0-9_] 统一替换为 _
    name = re.sub(r'\W', '_', name)
    # 避免纯数字或空字符串导致的奇怪问题
    return name or "import_db"

db_name = sanitize_db_name(LOCAL_CSV_DIR.name)

#服务器上的临时目录地址
REMOTE_TMP_DIR = "/tmp/csvs"

#数据库服务器的配置
servers = [
    {
        "ip": "111.111.111.111",
        "port": 22,
        "username": "postgresql",
        "password": "123456",
        "psql": "psql",  # 在 PATH 中
        "pg_port": 5432,
    },
    {
        "ip": "111.111.111.111",
        "port": 22,
        "username": "postgresql",
        "password": "123456",
        "psql": "psql",
        "pg_port": 5432,
    }
]

# ——————— 数据类型映射函数 ———————
def dtype_to_sql(dtype) -> str:
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    if pd.api.types.is_float_dtype(dtype):
        return 'DOUBLE PRECISION'
    if pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    if pd.api.types.is_categorical_dtype(dtype):
        return 'VARCHAR'
    if pd.api.types.is_object_dtype(dtype):
        return 'VARCHAR'
    return 'TEXT'

# ——————— 检测是否存在对应数据库，不存在则创建 ———————
def csv_create_table_sql(csv_path: Path) -> str:
    """
    根据 CSV 文件前几行推断列类型，生成：
      CREATE TABLE IF NOT EXISTS "table_name" ( ... );
    """
    table = csv_path.stem
    # 只读前 1000 行以加速类型推断，避免 low_memory 警告
    df = pd.read_csv(csv_path, nrows=1000, low_memory=False)
    cols = [
        f'"{col}" {dtype_to_sql(df[col].dtype)}'
        for col in df.columns
    ]
    cols_sql = ",\n  ".join(cols)
    return (
        f'CREATE TABLE IF NOT EXISTS "{table}" (\n'
        f'  {cols_sql}\n'
        f');'
    )

# ——————— SSH 执行辅助 ———————
def run_ssh_cmd(ssh: paramiko.SSHClient, cmd: str, print_cmd=True):
    if print_cmd:
        print(">>>", cmd)
    stdin, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.read().decode(errors="ignore").strip()
    err = stderr.read().decode(errors="ignore").strip()
    exit_status = stdout.channel.recv_exit_status()
    print(f"[DEBUG] exit={exit_status}")
    if out:
        print(f"[STDOUT]\n{out}")
    if err:
        print(f"[STDERR]\n{err}")
    return exit_status, out, err

# ——————— psql 命令构建函数 ———————
def psql_cmd(server: dict, db: str, sql: str) -> str:
    # 去掉 -q，保留 tA，这样输出也能看到
    return (
        f'{server["psql"]} -p {server["pg_port"]} -d {db} '
        f'-v ON_ERROR_STOP=1 -X -tA -c "{sql}"'
    )

# ——————— 数据库保障 ———————
def ensure_database(ssh: paramiko.SSHClient, server: dict, target_db: str):
    # 1) 在 postgres 库中检查是否存在（使用 ILIKE 实现大小写不敏感匹配）
    check_sql = f"SELECT 1 FROM pg_database WHERE datname ILIKE '{target_db}';"  # 修改这里：= 改为 ILIKE
    cmd_check = psql_cmd(server, "postgres", check_sql)
    code, out, err = run_ssh_cmd(ssh, cmd_check)
    if code != 0:
        print(f'[{server["ip"]}] 检查数据库失败: {err or out}')
        raise RuntimeError("数据库检查失败")

    # 检查输出是否包含查询结果（处理可能的大小写差异导致的匹配问题）
    if out.strip() == "1" or "1" in out.strip():
        print(f'[{server["ip"]}] 数据库 "{target_db}" 已存在')
        return  # 数据库存在时直接返回，跳过创建步骤

    # 2) 不存在则创建（UTF8 + template0，避免区域设定差异）
    create_sql = f'CREATE DATABASE "{target_db}" WITH ENCODING \'UTF8\' TEMPLATE template0;'
    cmd_create = psql_cmd(server, "postgres", create_sql)
    code, out, err = run_ssh_cmd(ssh, cmd_create)
    if code != 0:
        print(f'[{server["ip"]}] 创建数据库失败: {err or out}')
        raise RuntimeError("创建数据库失败")
    print(f'[{server["ip"]}] 已创建数据库 "{target_db}"')


# ——————— 部署 & 导入 ———————
def deploy_and_import(server: dict):
    ip = server['ip']
    print(f"=== [{ip}] Start ===")
    # 1. 建立 SSH + SFTP
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, server['port'], server['username'], server['password'])
    sftp = ssh.open_sftp()
    ssh.exec_command(f"mkdir -p {REMOTE_TMP_DIR}")

    # 2. 确保数据库存在
    ensure_database(ssh, server, db_name)

    # 3. 遍历本地所有 CSV
    for csv_file in LOCAL_CSV_DIR.glob("*.csv"):
        tbl = csv_file.stem
        remote_path = f"{REMOTE_TMP_DIR}/{csv_file.name}"

        # 上传
        sftp.put(str(csv_file), remote_path)
        print(f"[{ip}] 上传 {csv_file.name}")

        # 生成建表 + \copy 语句
        create_sql = csv_create_table_sql(csv_file)
        #-------------------------------
        create_table_cmd = (
            f"{server['psql']} -p {server['pg_port']} -d {db_name} -c "
            f"\"{create_sql}\""
        )
        code, out, err = run_ssh_cmd(ssh, create_table_cmd)
        if code != 0:
            print(f"[{ip}] 创建表 {tbl} 失败: {err or out}")
            # 删除远程临时文件
            ssh.exec_command(f"rm {remote_path}")
            continue
        # -------------------------------

        # 执行 COPY 命令导入数据
        copy_cmd = (
            f"{server['psql']} -p {server['pg_port']} -d {db_name} -c "
            f"\"COPY \\\"{tbl}\\\" FROM '{remote_path}' WITH (FORMAT csv, HEADER true, NULL 'NULL');\""
        )
        code, out, err = run_ssh_cmd(ssh, copy_cmd)
        if code != 0:
            print(f"[{ip}] 导入 {tbl} 失败: {err or out}")
        else:
            print(f"[{ip}] 导入 {tbl} 成功")

        # 执行 COPY 命令导入数据
        # copy_cmd = (
        #     f"COPY \"{tbl}\" "
        #     f"FROM '{remote_path}' "
        #     f"WITH (FORMAT csv, HEADER, NULL 'NULL');"
        # )
        #
        # # 然后 full_cmd 变成：
        # full_cmd = (
        #     f"{server['psql']} -c "
        #     f"\"{create_sql} {copy_cmd}\""
        # )
        #
        # stdin, stdout, stderr = ssh.exec_command(full_cmd)
        # err = stderr.read().decode().strip()
        # if err:
        #     print(f"[{ip}] 导入 {tbl} 失败: {err}")
        # else:
        #     print(f"[{ip}] 导入 {tbl} 成功")

        # 删除远程临时文件
        ssh.exec_command(f"rm {remote_path}")

    # 收尾
    sftp.close()
    ssh.close()
    print(f"=== [{ip}] Done ===")

# ——————— 并行入口 ———————
def main():
    with ThreadPoolExecutor(max_workers=len(servers)) as executor:
        executor.map(deploy_and_import, servers)

if __name__ == "__main__":
    main()
