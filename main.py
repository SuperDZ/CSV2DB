import re
import matplotlib.pyplot as plt
from matplotlib import rcParams
import os
import paramiko
import time
from sshtunnel import SSHTunnelForwarder
import psycopg2
import subprocess
import numpy as np
import matplotlib.font_manager as fm
from matplotlib.ticker import MultipleLocator,AutoMinorLocator

# ===== SSH 配置 =====
SSH_HOST1 = "192.168.31.20"
SSH_HOST2 = "192.168.31.10"
SSH_PORT = 22
SSH_USER = "postgresql"
SSH_PWD  = "md5654852"

# ===== PG 配置 =====
# 注意：这里填远端 PG 实际监听的地址（通常是 127.0.0.1 或 0.0.0.0）
REMOTE_BIND_ADDRESS = ("127.0.0.1", 5432)

PG_USER1    = "postgres"
PG_USER2    = "postgres"
PG_PWD      = ""
PG_DATABASE = "postgres"  # 一定要改成你的库名

# 设置中文字体，避免中文乱码
def set_chinese_font():
    # 优先尝试微软雅黑，其次黑体
    font_list = [
        'Microsoft YaHei', 'SimHei', 'STHeiti', 'Arial Unicode MS', 'PingFang SC'
    ]
    found = False
    for font in font_list:
        if font in [f.name for f in fm.fontManager.ttflist]:
            rcParams['font.family'] = font
            found = True
            break
    if not found:
        # fallback: 使用系统默认字体
        rcParams['font.family'] = 'sans-serif'
    rcParams['axes.unicode_minus'] = False  # 解决负号 '-' 显示成方块的问题

    # 强制使用特定字体以确保中文显示正确
    rcParams['font.sans-serif'] = ['SimHei']  # 使用黑体
    rcParams['font.serif'] = ['SimSun']  # 使用宋体

set_chinese_font()

# def parse_log_file(filepath):
#     # 每个模式匹配：先 statement: …; 然后换行，再 duration: X ms
#     ddl_patterns = {
#         'MODIFY COMMENT':
#         r"statement:\s*(?:COMMENT ON COLUMN.*?;|ALTER TABLE .* MODIFY .* COMMENT.*?;)"
#         r"\s*[\r\n]+.*?duration:\s*([\d\.]+)\s*ms",
#
#         'MODIFY SET NOT NULL':
#         r"statement:\s*(?:ALTER TABLE .* ALTER COLUMN .* SET NOT NULL;|ALTER TABLE .* MODIFY .* NOT NULL;)"
#         r"\s*[\r\n]+.*?duration:\s*([\d\.]+)\s*ms",
#
#         'MODIFY SET DEFAULT':
#         r"statement:\s*(?:ALTER TABLE .* ALTER COLUMN .* SET DEFAULT.*?;|ALTER TABLE .* MODIFY .* DEFAULT.*?;)"
#         r"\s*[\r\n]+.*?duration:\s*([\d\.]+)\s*ms",
#
#         'MODIFY PRIMARY KEY':
#         r"statement:\s*(?:ALTER TABLE .* ADD CONSTRAINT .* PRIMARY KEY.*?;|ALTER TABLE .* MODIFY .* PRIMARY.*?;)"
#         r"\s*[\r\n]+.*?duration:\s*([\d\.]+)\s*ms",
#
#         'MODIFY UNIQUE':
#         r"statement:\s*(?:ALTER TABLE .* ADD CONSTRAINT .* UNIQUE.*?;|ALTER TABLE .* MODIFY .* UNIQUE.*?;)"
#         r"\s*[\r\n]+.*?duration:\s*([\d\.]+)\s*ms",
#
#         'MODIFY TYPE':
#         r"statement:\s*(?:"
#           r"ALTER TABLE .* ALTER COLUMN .* TYPE (?:integer|double precision|numeric\(\d+,\d+\)|text|varchar\(\d+\));|"
#           r"ALTER TABLE .* MODIFY .*?(?:TYPE\s+)?(?:integer|double precision|numeric\(\d+,\d+\)|text|varchar\(\d+\));)"
#         r"\s*[\r\n]+.*?duration:\s*([\d\.]+)\s*ms",
#
#         'AUTO_INCREMENT':
#         r"statement:\s*(?:ALTER TABLE .* MODIFY .* AUTO_INCREMENT.*?;|ALTER TABLE .* ALTER COLUMN .* ADD GENERATED ALWAYS AS IDENTITY.*?;)"
#         r"\s*[\r\n]+.*?duration:\s*([\d\.]+)\s*ms",
#     }
#
#     patterns = {k: re.compile(v, re.IGNORECASE | re.DOTALL) for k, v in ddl_patterns.items()}
#     results = {k: [] for k in ddl_patterns}
#
#     text = open(filepath, 'r', encoding='utf-8').read()
#     for name, pat in patterns.items():
#         results[name] = [float(m) for m in pat.findall(text)]
#     return results



def parse_log_file(filepath):
    ddl_patterns = {
        'MODIFY COMMENT':      r'duration:\s*([\d\.]+)\s*ms\s*statement:\s*(?:COMMENT ON COLUMN.*?;|ALTER TABLE .* MODIFY .* COMMENT.*?;)',
        'MODIFY NOT NULL': r'duration:\s*([\d\.]+)\s*ms\s*statement:\s*(?:ALTER TABLE .* ALTER COLUMN .* SET NOT NULL;|ALTER TABLE .* MODIFY .* NOT NULL;)',
        'MODIFY SET DEFAULT':  r'duration:\s*([\d\.]+)\s*ms\s*statement:\s*(?:ALTER TABLE .* ALTER COLUMN .* SET DEFAULT.*?;|ALTER TABLE .* MODIFY .* DEFAULT.*?;)',
        'MODIFY PRIMARY KEY':  r'duration:\s*([\d\.]+)\s*ms\s*statement:\s*(?:ALTER TABLE .* ADD CONSTRAINT .* PRIMARY KEY.*?;|ALTER TABLE .* MODIFY .* PRIMARY.*?;)',
        'MODIFY UNIQUE':       r'duration:\s*([\d\.]+)\s*ms\s*statement:\s*(?:ALTER TABLE .* ADD CONSTRAINT .* UNIQUE.*?;|ALTER TABLE .* MODIFY .* UNIQUE.*?;)',
        'MODIFY TYPE':         r'duration:\s*([\d\.]+)\s*ms\s*statement:\s*(?:ALTER TABLE .* ALTER COLUMN .* TYPE .*?;|ALTER TABLE .* MODIFY .* (?:integer|double precision|numeric\(\d+,\d+\)|text|varchar\(\d+\));)',
        'AUTO_INCREMENT':      r'duration:\s*([\d\.]+)\s*ms\s*statement:\s*(?:ALTER TABLE .* MODIFY .* AUTO_INCREMENT.*?;|ALTER TABLE .* ALTER COLUMN .* ADD GENERATED ALWAYS AS IDENTITY.*?;)',
    }

    patterns = {k: re.compile(v, re.IGNORECASE) for k, v in ddl_patterns.items()}
    results = {k: [] for k in ddl_patterns}

    text = open(filepath, 'r', encoding='utf-8').read()
    for name, pat in patterns.items():
        matches = pat.findall(text)
        results[name] = [float(ms) for ms in matches]
    # 过滤 MODIFY SET DEFAULT 中大于 100 ms 的数据
    results['MODIFY NOT NULL'] = [ms for ms in results['MODIFY NOT NULL'] if ms <= 100]
    results['MODIFY SET DEFAULT'] = [ms for ms in results['MODIFY SET DEFAULT'] if ms <= 4]
    results['MODIFY TYPE'] = [ms for ms in results['MODIFY TYPE'] if ms >= 100]
    results['MODIFY TYPE'] = [ms for ms in results['MODIFY TYPE'] if ms <= 400]
    results['MODIFY COMMENT'] = [ms for ms in results['MODIFY COMMENT'] if ms <=5]
    results['MODIFY PRIMARY KEY'] = [ms for ms in results['MODIFY PRIMARY KEY'] if ms <= 120]
    results['MODIFY UNIQUE'] = [ms for ms in results['MODIFY UNIQUE'] if ms <= 110]
    return results

def average(lst):
    if not lst:
        return 0
    lst = lst.copy()
    lst.remove(max(lst)) if len(lst) > 1 else lst
    return sum(lst) / len(lst) if lst else 0


def paint_main():
    log_dir = r'C:\\Users\\MZJ-Y9000P\\Desktop\\fsdownload'
    before_log = os.path.join(log_dir, 'postgresql20-2025-08-31_032259.log')
    after_log = os.path.join(log_dir, 'postgresql10-2025-08-31_032306.log')

    before_data = parse_log_file(before_log)
    after_data = parse_log_file(after_log)

    summary = {}
    for operation in before_data.keys():
        summary[operation] = {
            'before_avg': average(before_data[operation]),
            'after_avg': average(after_data[operation]),
            'before_times': before_data[operation],
            'after_times': after_data[operation]
        }

    for op, times in summary.items():
        print(f"\n### {op}")
        print(f"- 修改前：{times['before_times']} -> 平均 {times['before_avg']:.3f} ms")
        print(f"- 修改后：{times['after_times']} -> 平均 {times['after_avg']:.3f} ms")
        print(f"- 修改前： 平均 {times['before_avg']:.3f} ms")
        print(f"- 修改后： 平均 {times['after_avg']:.3f} ms")



    # 科研绘图风格的柱状图
    labels = list(summary.keys())
    before = [summary[op]['before_avg'] for op in labels]
    after = [summary[op]['after_avg'] for op in labels]

    x = np.arange(len(labels))  # 柱状图的x轴位置
    width = 0.35  # 柱宽

    # 整体风格
    plt.style.use('seaborn-v0_8-whitegrid')
    rcParams['font.family'] = 'SimHei'
    rcParams['axes.unicode_minus'] = False

    # 1. 建两个子图，上大下小
    fig, (ax_high, ax_low) = plt.subplots(
        2, 1, sharex=True,
        figsize=(12, 8),
        gridspec_kw={'height_ratios': [1, 3], 'hspace': 0.06}
    )
    # 2. 上：大值区间；下：小值区间
    ax_high.bar(x - width / 2, before, width, label='修改前',
                color='#66C2A5', edgecolor='k', hatch='/')
    ax_high.bar(x + width / 2, after, width, label='修改后',
                color='#E78AC3', edgecolor='k', hatch='\\')
    ax_high.set_ylim(220, 320)

    ax_low.bar(x - width / 2, before, width, color='#66C2A5', edgecolor='k', hatch='/')
    ax_low.bar(x + width / 2, after, width, color='#E78AC3', edgecolor='k', hatch='\\')
    ax_low.set_ylim(0, 82)

    # 隐藏相接的框线
    ax_high.spines['bottom'].set_visible(False)
    ax_low.spines['top'].set_visible(False)

    # 3. 在画布上渲染一次，拿到像素尺寸
    fig.canvas.draw()
    bh = ax_high.get_window_extent().height
    bw = ax_high.get_window_extent().width
    lh = ax_low.get_window_extent().height
    lw = ax_low.get_window_extent().width

    # 我们希望“像素斜率”相同：dy_pix = dx_pix
    # 选一个小偏移量 dx_frac 在 Axes 坐标系（比例坐标）：
    dx = 0.01
    # 计算 dy_frac_high * bh = dx * bw  => dy_frac_high = dx * bw / bh
    dy_high = dx * (bw / bh)
    dy_low = dx * (lw / lh)

    # 4. 画斜线断层
    kw = dict(color='k', clip_on=False)
    # 上半图斜线（同样的 dx_frac, dy_frac_high）
    ax_high.plot((-dx, +dx), (-dy_high, +dy_high),
                 transform=ax_high.transAxes, **kw)
    ax_high.plot((1 - dx, 1 + dx), (-dy_high, +dy_high),
                 transform=ax_high.transAxes, **kw)
    # 下半图斜线（同样的 dx_frac, dy_frac_low）
    ax_low.plot((-dx, +dx), (1 - dy_low, 1 + dy_low),
                transform=ax_low.transAxes, **kw)
    ax_low.plot((1 - dx, 1 + dx), (1 - dy_low, 1 + dy_low),
                transform=ax_low.transAxes, **kw)

    # 5. 四周框线 & 主次刻度
    for ax in (ax_high, ax_low):
        ax.grid(False)
        # 只保留可见的脊线对应的刻度
        # 上图：去掉 x 轴刻度底部
        # 下图：去掉 x 轴刻度顶部
        if ax is ax_high:
            ax.tick_params(axis='x', which='both', bottom=False, labelbottom=False)
        else:
            ax.tick_params(axis='x', which='both', top=False)

        # y 轴刻度保留在左侧
        ax.yaxis.set_ticks_position('left')
    # 次刻度
    ax.minorticks_on()
    ax.xaxis.set_minor_locator(AutoMinorLocator(3))
    ax.yaxis.set_minor_locator(AutoMinorLocator(2))
    ax.tick_params(which='major',direction='out',length=4,width=1,pad=8)
    ax.tick_params(which='minor',direction='out',length=2,width=0.8,pad=4)

    # 6. 只在下半图设置 x 轴标签
    ax_low.set_xticks(x)
    ax_low.set_xticklabels(labels, rotation=30, ha='right', fontsize=12)
    # 1. 给上半图设置主刻度间隔（比如每 10 ms 一格）
    ax_high.yaxis.set_major_locator(MultipleLocator(40))
    # 2. 给上半图也打开次刻度
    ax_high.yaxis.set_minor_locator(AutoMinorLocator(2))
    ax_high.tick_params(which='major',direction='out',length=4,width=1,pad=8)
    ax_high.tick_params(which='minor',direction='out',length=2,width=0.8,pad=4)


    # 7. 添加共享的、垂直居中的 y 轴标签
    #    用 fig.text 放在画布左侧 4% 处，高度 50%
    fig.text(
        0.04, 0.5, '耗时 (ms)',
        va='center', rotation='vertical',
        fontproperties='SimHei', fontsize=14
    )
    # 7. 把所有脊线都放到最下面一层
    for ax in (ax_high, ax_low):
        for spine in ax.spines.values():
            spine.set_zorder(1)
            spine.set_color('black')


    # 8. 图例放到上半图框外
    leg = ax_high.legend(
        loc='lower center', bbox_to_anchor=(0.5, 1.05),
        ncol=2, frameon=False, fontsize=20,
        handlelength=6, handleheight=2, handletextpad=2,
        columnspacing=8, borderpad=0.2,
        prop={'family': 'SimHei'}
    )
    plt.subplots_adjust(
        top=0.92,  # 顶部留给图例
        bottom=0.16,  # 底部留给 x 轴标签
        left=0.09,  # 左侧留给 y 轴标签
        right=0.97  # 右侧留点空白
    )

    plt.tight_layout()
    plt.show()
    fig.savefig(
        "ddl_timing_comparison.pdf",  # 输出文件名
        format="pdf",  # 格式
        bbox_inches="tight"  # 紧凑地裁剪多余空白
    )


if __name__ == "__main__":
    # LOCAL_PORT_BEFORE = 3308
    # LOCAL_PORT_AFTER = 3310

    # print("==== 执行修改前 SQL 脚本 ====")
    # run_script_over_ssh_tunnel(SSH_HOST1, LOCAL_PORT_BEFORE, PG_USER2, 'C:\\Users\\MZJ-Y9000P\\Desktop\\fsdownload\\test_sql_before.sql')
    # time.sleep(5)  # 添加延迟以确保第一个隧道完全关闭

    # print("==== 执行修改后 SQL 脚本 ====")
    # run_script_over_ssh_tunnel(SSH_HOST2, LOCAL_PORT_AFTER, PG_USER2, 'C:\\Users\\MZJ-Y9000P\\Desktop\\fsdownload\\test_sql_after.sql')
    paint_main()

