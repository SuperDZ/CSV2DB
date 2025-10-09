#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
runner_from_conf.py —— 自动读取同目录下的 .conf（无命令行参数），直接在 main() 运行

设计要点
- 不再在 main() 中硬编码参数；自动扫描脚本目录下的 .conf 文件读取参数；
- 兼容两类配置：
  (A) 你现有的简版：[Server1]/[Server2] 格式，字段示例：
      ip, port(SSH), username(SSH), password(SSH/PG), pg_port, sql_file_path,
      psql_command（例如：psql -d dbname -p 5432），psql_close_command（可选停库命令）
  (B) 可选 [Runner] 段用于覆盖 repeat/retry/timeout/remote_tmp_dir/pre_checkpoint/save_local 等；
- 实现非交互 psql 执行、失败即停、repeat+重试、结构化日志(远端 JSONL + meta.json)；
- 可选在批次开始前执行 CHECKPOINT；可选在全部完成后执行 psql_close_command。

注意
- 若同目录存在多个 .conf：优先使用 server_config.conf；否则使用第一个找到的 .conf；
  也可通过环境变量 RUNNER_CONF 指定特定配置文件路径。
- psql_command 若已包含 -d/-p/-h/-U，将按原样执行，并追加固定参数(-X -q -At -v ON_ERROR_STOP=1 -P pager=off)；
- 如 password 配置了，将以环境变量 PGPASSWORD 传入远端会话（建议生产使用 .pgpass）。
"""

from __future__ import annotations
import os
import json
import time
import paramiko
import configparser
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

PSQL_ARGS = "-X -q -At -v ON_ERROR_STOP=1 -P pager=off"

@dataclass
class RunnerCfg:
    repeat: int = 2
    retry: int = 2
    ssh_timeout: int = 10
    banner_timeout: int = 10
    stmt_timeout_ms: int = 300000
    lock_timeout_ms: int = 5000
    remote_tmp_dir: str = "/tmp"
    pre_checkpoint: bool = False
    save_local: bool = False

@dataclass
class ServerCfg:
    name: str
    ip: str
    ssh_port: int
    username: str
    password: str
    pg_port: int
    sql_file_path: str
    psql_command: str  # 基础 psql 命令行（例如：psql -d db -p 5432）
    psql_close_command: Optional[str] = None
    remote_tmp_dir: Optional[str] = None  # 可覆盖 runner.remote_tmp_dir

def choose_conf_file() -> Path:
    # 1) 环境变量优先
    env_path = os.environ.get("RUNNER_CONF")
    if env_path and Path(env_path).exists():
        return Path(env_path)
    # 2) 同目录 server_config.conf 优先
    base = Path(__file__).parent
    sc = base / "server_config.conf"
    if sc.exists():
        return sc
    # 3) 其它 .conf（取第一个）
    confs = sorted(base.glob("*.conf"))
    if not confs:
        raise SystemExit("未在脚本目录找到 .conf 配置文件。可将配置命名为 server_config.conf 或设置环境变量 RUNNER_CONF。")
    return confs[0]

def read_conf(path: Path) -> tuple[RunnerCfg, List[ServerCfg]]:
    cp = configparser.ConfigParser()
    cp.read(path, encoding="utf-8")

    rcfg = RunnerCfg()
    if cp.has_section("Runner"):
        g = cp["Runner"]
        rcfg.repeat = int(g.get("repeat", rcfg.repeat))
        rcfg.retry = int(g.get("retry", rcfg.retry))
        rcfg.ssh_timeout = int(g.get("ssh_timeout", rcfg.ssh_timeout))
        rcfg.banner_timeout = int(g.get("banner_timeout", rcfg.banner_timeout))
        rcfg.stmt_timeout_ms = int(g.get("stmt_timeout_ms", rcfg.stmt_timeout_ms))
        rcfg.lock_timeout_ms = int(g.get("lock_timeout_ms", rcfg.lock_timeout_ms))
        rcfg.remote_tmp_dir = g.get("remote_tmp_dir", rcfg.remote_tmp_dir)
        rcfg.pre_checkpoint = g.getboolean("pre_checkpoint", rcfg.pre_checkpoint)
        rcfg.save_local = g.getboolean("save_local", rcfg.save_local)

    servers: List[ServerCfg] = []
    for sec in cp.sections():
        if not sec.lower().startswith("server"):
            continue
        s = cp[sec]
        servers.append(ServerCfg(
            name = sec,
            ip = s.get("ip", "127.0.0.1"),
            ssh_port = int(s.get("port", "22")),
            username = s.get("username", "postgres"),
            password = s.get("password", ""),
            pg_port = int(s.get("pg_port", "5432")),
            sql_file_path = s.get("sql_file_path", "./queries/demo.sql"),
            psql_command = s.get("psql_command", "psql"),
            psql_close_command = s.get("psql_close_command", None),
            remote_tmp_dir = s.get("remote_tmp_dir", None),
        ))
    if not servers:
        raise SystemExit("配置文件中未找到 [Server...] 段落。请参考示例：\n[Server1]\nip=...\nport=22\nusername=...\nsql_file_path=...\npsql_command=psql -d db -p 5432")
    return rcfg, servers

def ssh_connect(ip: str, port: int, username: str, password: str, timeout: int, banner_timeout: int) -> paramiko.SSHClient:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, port=port, username=username, password=password or None,
                timeout=timeout, banner_timeout=banner_timeout)
    return ssh

def sftp_write_text(sftp, remote_path: str, text: str):
    with sftp.file(remote_path, "a") as f:
        f.write(text)

def run_cmd(ssh: paramiko.SSHClient, cmd: str, env: Optional[Dict[str,str]] = None, timeout: int = 1800):
    t0 = time.time()
    stdin, stdout, stderr = ssh.exec_command(cmd, get_pty=False, environment=env, timeout=timeout)
    out = stdout.read().decode(errors="ignore")
    err = stderr.read().decode(errors="ignore")
    code = stdout.channel.recv_exit_status()
    return code, out, err, round(time.time() - t0, 3)

def make_remote_run_dir(base_dir: str) -> str:
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"{base_dir.rstrip('/')}/run_{ts}_{int(time.time()*1000)}"

def build_psql_cmd(psql_command: str) -> str:
    # 把固定参数拼到用户给的基础命令后面；用户命令里可含 -d/-p/-h/-U 等
    if PSQL_ARGS in psql_command:
        return psql_command
    return f"{psql_command} {PSQL_ARGS}"

def run_server(s: ServerCfg, rcfg: RunnerCfg) -> Dict:
    ssh = None
    meta = {
        "server": s.name, "ip": s.ip, "ssh_port": s.ssh_port, "username": s.username,
        "pg_port": s.pg_port, "sql_file_path": s.sql_file_path,
        "psql_command": s.psql_command, "repeat": rcfg.repeat, "retry": rcfg.retry,
        "stmt_timeout_ms": rcfg.stmt_timeout_ms, "lock_timeout_ms": rcfg.lock_timeout_ms
    }
    try:
        ssh = ssh_connect(s.ip, s.ssh_port, s.username, s.password, rcfg.ssh_timeout, rcfg.banner_timeout)
        sftp = ssh.open_sftp()
        run_dir = make_remote_run_dir(s.remote_tmp_dir or rcfg.remote_tmp_dir)
        try: sftp.mkdir(run_dir)
        except: pass

        # 确保本地 SQL 存在
        sql_local = Path(s.sql_file_path).expanduser().resolve()
        if not sql_local.exists():
            # 允许空文件：生成 demo，避免失败
            sql_local.parent.mkdir(parents=True, exist_ok=True)
            sql_local.write_text("-- demo sql\nSELECT 1;\n", encoding="utf-8")

        # 上传 SQL 与生成 wrapper（注入超时）
        remote_sql = f"{run_dir}/{sql_local.name}"
        sftp.put(str(sql_local), remote_sql)

        wrapper = f"{run_dir}/wrapper.sql"
        setup = (
            f"SET lock_timeout = '{rcfg.lock_timeout_ms}ms';\n"
            f"SET statement_timeout = '{rcfg.stmt_timeout_ms}ms';\n"
            f"SET idle_in_transaction_session_timeout = '5min';\n"
            f"\\i {remote_sql}\n"
        )
        with sftp.file(wrapper, "w") as fh:
            fh.write(setup)

        # 批次开始前可选 CHECKPOINT
        if rcfg.pre_checkpoint:
            ck = build_psql_cmd(s.psql_command) + ' -c "CHECKPOINT"'
            run_cmd(ssh, ck, env={"PGPASSWORD": s.password} if s.password else None, timeout=120)

        # 执行 repeat 次
        results = []
        psql_cmd = build_psql_cmd(s.psql_command) + f' --file="{wrapper}"'
        for r in range(1, rcfg.repeat + 1):
            attempts = 0
            while True:
                attempts += 1
                code, out, err, sec = run_cmd(ssh, psql_cmd,
                                              env={"PGPASSWORD": s.password} if s.password else None,
                                              timeout=rcfg.stmt_timeout_ms//1000 + 60)
                rec = {"round": r, "attempt": attempts, "exit": code, "elapsed_sec": sec, "stderr": (err or "")[:4000]}
                results.append(rec)
                sftp_write_text(sftp, f"{run_dir}/results.jsonl", json.dumps(rec, ensure_ascii=False) + "\n")
                if code == 0: break
                if attempts > rcfg.retry:
                    raise RuntimeError(f"psql failed after {attempts} attempts (round {r}): {err[:500]}")
                time.sleep(min(2*attempts, 10))

        # 写元数据
        with sftp.file(f"{run_dir}/meta.json", "w") as fh:
            fh.write(json.dumps(meta, ensure_ascii=False, indent=2))

        # 可选停库（若配置提供了 close 命令）
        if s.psql_close_command:
            run_cmd(ssh, s.psql_close_command, timeout=120)

        # 可选本地保存摘要
        if rcfg.save_local:
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            ldir = Path(f"./runs/{ts}/{s.name}")
            ldir.mkdir(parents=True, exist_ok=True)
            (ldir/"meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
            (ldir/"results.json").write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")

        return {"server": s.name, "remote_dir": run_dir, "results_n": len(results)}

    except Exception as e:
        return {"server": s.name, "error": str(e)}
    finally:
        try:
            if ssh: ssh.close()
        except Exception:
            pass

def main():
    conf = choose_conf_file()
    print(f"[runner] use config: {conf}")
    rcfg, servers = read_conf(conf)
    # 顺序执行（如需并发可自行改为线程池；DDL 建议同库串行）
    all_res = []
    for s in servers:
        print(f"[{s.name}] connecting {s.ip}:{s.ssh_port} sql={s.sql_file_path}")
        res = run_server(s, rcfg)
        all_res.append(res)
        if "error" in res:
            print(f"[{s.name}] ERROR: {res['error']}")
        else:
            print(f"[{s.name}] OK -> {res['remote_dir']}")
    print(json.dumps(all_res, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
