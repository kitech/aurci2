#!/usr/bin/env python3
"""解析 GitHub Actions 构建日志，输出失败包、详细错误、归属分类。

用法:
  ./ci-log-analyze.py ~/Downloads/logs_117974105767.zip
  ./ci-log-analyze.py 0_build_repository.txt [--repo DIR] [--json]
                      [--only ours|external|pseudo|all]
                      [--max-errors N] [--tail N] [--width N]
                      [--line-numbers]

输出:
  - 总表：包名 / 版本 / 结果 / 归属(ours|external|pseudo) / 失败次数
  - 每个失败包：每次尝试的时间、耗时、失败阶段、错误直方图
    + 去重后的错误详情（含日志行号、file:line、caret 上下文）
    + 失败前最后 N 行原文

退出码: 0 全部通过; 1 存在自有包失败; 2 仅外部/假包失败。
"""
import argparse
import json
import re
import sys
import tempfile
import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------- 解析规则
TS = re.compile(r'^\d{4}-\d\d-\d\dT[\d:.]+Z ?')
TS_FULL = re.compile(r'^(\d{4}-\d\d-\d\dT[\d:.]+?)Z ?(.*)$')
MAKE = re.compile(r'==> Making package: (\S+) (\S+)')
DONE = re.compile(r'==> Finished making: (\S+)')
FAIL = re.compile(r'Warning: Build failed for (\S+)')
BUILDING = re.compile(r'^Building (\S+)\.\.\.$')
PHASE = re.compile(r'==> Starting (\w+)\(\)\.\.\.')
XTRACE = re.compile(r'^\+ ')
NOISE = re.compile(r'^\s*$|^\+\s|downloading\.\.\.|^\s*\|\s|^\[?\d+$|^\s*\d+%')

# 错误锚点：(类别, 正则)；顺序即优先级
ANCHORS = [
    ('rust',    re.compile(r'\berror\[E\d+\]:')),
    ('cc',      re.compile(r'fatal error:|: error:|\berror:')),
    ('link',    re.compile(r'undefined reference to|undefined symbol')),
    ('cmake',   re.compile(r'CMake Error|Could NOT find|NOTFOUND')),
    ('pkg',     re.compile(r'One or more files did not pass|could not be found|'
                           r'corrupted|checksum|Could not open file|'
                           r'failed retrieving file|Could resolve all dependencies')),
    ('make',    re.compile(r'^\s*make(\[\d+\])?: \*\*\*|qmake failed')),
    ('fatal',   re.compile(r'^==> ERROR:|fatal:')),
]
# 上下文续行：rust 的 --> 定位、gcc 的 caret 行、链接符号的具体符号名
CONT_AFTER = re.compile(r'^\s*(-->|\||\d+\s*\||\^~*|note:|help:|'
                        r'[0-9]+ \||Caused by:|required by:|Please set|'
                        r'used as|linked by|CLANG_|LLVM_)')
CONT_BEFORE = re.compile(r'^(/|\./|src/)[^\s]+:\d+')

# 归一键：去掉日志前缀/ANSI/多余空白，路径只留 basename，便于跨尝试去重
ANSI = re.compile(r'\x1b\[[0-9;]*[A-Za-z]|\[[0-9;]*m')
BASENAME = re.compile(r'(?<![\w/])(/[^\s:]+/)([\w.+-]+:\d+(?::\d+)?)')
HEX = re.compile(r'0x[0-9a-fA-F]+')
PSEUDO = re.compile(r'^(target|not|AUR::Depends:|AUR::|found:|Depends:)$')


# ---------------------------------------------------------------- 输入/分类
def load(path: Path):
    if path.suffix == '.zip':
        tmp = Path(tempfile.mkdtemp(prefix='ci-log-'))
        with zipfile.ZipFile(path) as z:
            z.extractall(tmp)
        logs = sorted(tmp.rglob('*.txt'), key=lambda p: p.stat().st_size)
        return logs[-1].read_text(errors='replace')
    return path.read_text(errors='replace')


def classify(name: str, repo: Path):
    if PSEUDO.match(name):
        return 'pseudo'
    if (repo / 'pkgrepos' / name / 'PKGBUILD').is_file():
        return 'ours'
    return 'external'


# 行级精确分类：按具体特征判定，避免 pacman/cargo 的 "error:" 冒充编译错误
CLASSIFY = [
    ('pkg', re.compile(r'failed to commit transaction|failed retrieving file|'
                       r'Could not open file|corrupted|checksum|'
                       r'did not pass the validity check|'
                       r'Could resolve all dependencies|'
                       r'failed to install missing dependencies')),
    ('cargo', re.compile(r'failed to parse manifest|could not compile|'
                         r'error\[E\d+\]|no method named|mismatched types|'
                         r'has no field named|expected .+ found')),
    ('link', re.compile(r'undefined reference to|undefined symbol')),
    ('cmake', re.compile(r'CMake Error|Could NOT find|NOTFOUND|'
                         r'CMake Generate step|configure: error')),
    ('make', re.compile(r'qmake failed|make(\[\d+\])?: \*\*\*')),
    ('fatal', re.compile(r'^==> ERROR|^fatal:')),
    ('cc', re.compile(r'fatal error:|: error:')),
]


def classify_line(text: str) -> str:
    for cat, pat in CLASSIFY:
        if pat.search(text):
            return cat
    return 'other'


def norm(text: str) -> str:
    t = ANSI.sub('', text).strip()
    t = BASENAME.sub(r'\2', t)
    t = HEX.sub('0xADDR', t)
    return re.sub(r'\s+', ' ', t)


def parse_ts(s: str):
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    except ValueError:
        return None


def dur(a, b):
    if not a or not b:
        return ''
    sec = int((b - a).total_seconds())
    return f'{sec // 60}m{sec % 60:02d}s' if sec >= 60 else f'{sec}s'


# ---------------------------------------------------------------- 错误抽取
def extract_errors(episode, max_errors):
    """episode: [(lineno, text)]，返回 (errors, hist)。"""
    found = []                                    # (prio, lineno, lines[])
    for idx, (ln, text) in enumerate(episode):
        for prio, (_cat, pat) in enumerate(ANCHORS):
            if not pat.search(text):
                continue
            block = []
            # 前一行常是触发的文件/目标
            if idx > 0 and CONT_BEFORE.match(episode[idx - 1][1].strip()):
                block.append(episode[idx - 1][1])
            block.append(ANSI.sub('', text))
            # 后续 4 行保留 --> / caret / note
            blanks = 0
            for j in range(idx + 1, min(idx + 7, len(episode))):
                nxt = ANSI.sub('', episode[j][1])
                if not nxt.strip():
                    blanks += 1                    # 允许跨一个空行继续取 "Caused by:"
                    if blanks > 1:
                        break
                    continue
                if (CONT_AFTER.match(nxt)
                        or (block and block[-1].rstrip().endswith((':', ',')))):
                    block.append(nxt)
                    blanks = 0
                else:
                    break
            found.append((prio, ln, block))
            break

    errors, hist, seen = [], Counter(), {}
    seen_make = False
    for prio, ln, block in found:
        cat = classify_line(block[0])
        # make 的递归回溯只保留最深一层；"A failure occurred in X()" 与表头阶段重复
        if cat == 'make':
            if seen_make:
                continue
            seen_make = True
        if cat == 'fatal' and re.search(r'A failure occurred in \w+\(\)', block[0]):
            continue
        hist[cat] += 1
        key = norm(block[0])
        if key in seen:
            seen[key]['count'] += 1
            continue
        rec = {'category': cat, 'line': ln, 'count': 1,
               'detail': [x.rstrip() for x in block]}
        seen[key] = rec
        errors.append(rec)

    # 按出现顺序保留，最多 max_errors 组
    errors.sort(key=lambda e: e['line'])
    return errors[:max_errors], hist


# ---------------------------------------------------------------- 主解析
def analyze(text: str, max_errors: int):
    pkgs, cur, phase, t0, ep = {}, None, None, None, []
    total_errors = Counter()

    def rec_for(name):
        return pkgs.setdefault(name, {'ver': '?', 'ok': False, 'fails': 0,
                                      'attempts': []})

    for lineno, raw in enumerate(text.splitlines(), 1):
        m = TS_FULL.match(raw)
        ts, line = (parse_ts(m.group(1)), m.group(2)) if m else (None, TS.sub('', raw))
        if XTRACE.match(line):
            continue

        m = BUILDING.match(line)
        if m:
            cur, phase, t0, ep = m.group(1), None, ts, []
            continue

        m = MAKE.search(line)
        if m:
            cur, phase, t0, ep = m.group(1), None, ts, []
            rec_for(cur)['ver'] = m.group(2)
            continue

        m = PHASE.search(line)
        if m:
            phase = m.group(1)
            continue
        if 'Installing missing dependencies' in line:
            phase = 'deps(pacman)'
            continue
        if '==> Retrieving sources' in line:
            phase = 'sources'
            continue

        if DONE.search(line) and cur:
            rec_for(cur)['ok'] = True
            ep = []
            continue

        m = FAIL.search(line)
        if m:
            name = m.group(1).strip("'\"")
            rec = rec_for(name)
            rec['fails'] += 1
            errs, hist = extract_errors(ep, max_errors)
            total_errors.update(hist)
            tail = [ANSI.sub('', x[1]).rstrip() for x in ep[-5:]]
            rec['attempts'].append({
                'time': ts.strftime('%H:%M:%S') if ts else '?',
                'duration': dur(t0, ts),
                'phase': phase or '?',
                'errors': errs,
                'hist': dict(hist),
                'tail': tail,
            })
            cur, phase, t0, ep = None, None, None, []
            continue

        if cur is not None:
            ep.append((lineno, line))
            ep = ep[-600:]
    return pkgs, total_errors


# ---------------------------------------------------------------- 输出
def print_table(rows):
    print(f"{'包名':<28}{'版本':<22}{'结果':<7}{'归属':<10}次数")
    print('-' * 78)
    for name, r in rows:
        print(f"{name:<28}{r['ver'][:21]:<22}"
              f"{'OK' if r['ok'] else 'FAIL':<7}{r['kind']:<10}{r['fails']}")


def print_details(rows, max_errors, width, show_lineno, tail, show_pseudo=False):
    for name, r in rows:
        if r['ok']:
            continue
        if r['kind'] == 'pseudo' and not show_pseudo:
            continue                              # 伪包只在末尾汇总，不展开
        print(f"### {name}  [{r['kind']}]  {r['ver']}  失败 {r['fails']} 次")
        if r['kind'] == 'pseudo':
            print("  (工作流解析错误: aur depends 的报错文本被当成包名，"
                  "与包本身无关；--show-pseudo 才展开明细)")

        # 按错误签名聚合尝试：签名相同的多次只打一组
        groups, by_sig = [], {}
        for i, a in enumerate(r['attempts'], 1):
            sig = tuple(norm(e['detail'][0]) for e in a['errors'][:2])
            if sig not in by_sig:
                by_sig[sig] = []
                groups.append(sig)
            by_sig[sig].append((i, a))

        for sig in groups:
            members = by_sig[sig]
            first = members[0][1]
            idxs = ','.join(str(i) for i, _ in members)
            span = (f"{first['time']} ~ {members[-1][1]['time']}"
                    if len(members) > 1 else first['time'])
            ds = [a['duration'] or '?' for _, a in members]
            durs = f'{ds[0]} ×{len(ds)}' if len(set(ds)) == 1 else ' / '.join(ds)
            print(f"  [×{len(members)} 次 (尝试 {idxs})] {span}  "
                  f"耗时 {durs}  阶段 {first['phase']}")
            if first['errors']:
                h = '  '.join(f'{k}×{v}' for k, v in sorted(first['hist'].items()))
                print(f"    错误分布: {h}")
            else:
                print("    错误分布: (未匹配到错误锚点，见尾部原文)")
            for e in first['errors']:
                loc = f"L{e['line']}" if show_lineno else ''
                cnt = f" (×{e['count']})" if e['count'] > 1 else ''
                print(f"    - [{e['category']}] {loc}{cnt}")
                for d in e['detail']:
                    print(f"        {d[:width]}")
        if tail and r['attempts']:
            print("  失败前最后几行:")
            for t in r['attempts'][-1]['tail'][-tail:]:
                print(f"        {t[:width]}")
        print()


def pseudo_summary(pkgs):
    """伪包（工作流把 aur depends 报错当包名）汇总成一行。"""
    hits = [(n, r['fails']) for n, r in pkgs.items() if r['kind'] == 'pseudo']
    if not hits:
        return ''
    hits.sort(key=lambda kv: (-kv[1], kv[0]))
    total = sum(c for _, c in hits)
    return ('忽略的伪包名（工作流 aur depends 噪声，共 '
            f'{total} 次）: ' + ', '.join(f'{n} ×{c}' for n, c in hits) +
            '\n使用 --show-pseudo 查看明细')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('log')
    ap.add_argument('--repo', default=str(Path(__file__).resolve().parents[1]))
    ap.add_argument('--json', action='store_true')
    ap.add_argument('--max-errors', type=int, default=8,
                    help='每个包最多展示多少组去重错误 (默认 8)')
    ap.add_argument('--tail', type=int, default=6,
                    help='展示失败前最后几行原文 (默认 6, 0=关闭)')
    ap.add_argument('--width', type=int, default=300,
                    help='单行最大字符数 (默认 300)')
    ap.add_argument('--line-numbers', action='store_true',
                    help='标注错误在日志中的行号，便于 grep')
    ap.add_argument('--only', choices=['ours', 'external', 'pseudo', 'all'],
                    default='all', help='只输出某一归属的包 (默认 all)')
    ap.add_argument('--show-pseudo', action='store_true',
                    help='展开伪包(工作流噪声)的尝试明细，默认只汇总一行')
    a = ap.parse_args()

    text = load(Path(a.log).expanduser())
    pkgs, total_errors = analyze(text, a.max_errors)
    repo = Path(a.repo)
    for name, rec in pkgs.items():
        rec['kind'] = classify(name, repo)

    order = {'ours': 0, 'external': 1, 'pseudo': 2}
    rows = sorted(pkgs.items(),
                  key=lambda kv: (order[kv[1]['kind']], not kv[1]['ok'],
                                  -kv[1]['fails'], kv[0]))
    if a.only != 'all':
        rows = [(n, r) for n, r in rows if r['kind'] == a.only]

    # 统计只算真实包，伪包会污染直方图
    total_errors.clear()
    for name, rec in pkgs.items():
        if rec['kind'] == 'pseudo':
            continue
        for att in rec['attempts']:
            total_errors.update(att['hist'])

    if a.json:
        print(json.dumps({'packages': {n: r for n, r in rows},
                          'error_categories': dict(total_errors)},
                         indent=2, ensure_ascii=False))
    else:
        print_table(rows)
        print()
        print_details(rows, a.max_errors, a.width, a.line_numbers, a.tail,
                      a.show_pseudo)
        note = pseudo_summary(pkgs)
        if note:
            print(note)
            print()
        if total_errors:
            print('错误类别总计: ' +
                  '  '.join(f'{k}×{v}' for k, v in total_errors.most_common()))

    ours_bad = any(r['kind'] == 'ours' and not r['ok'] for r in pkgs.values())
    ext_bad = any(r['kind'] != 'ours' and not r['ok'] for r in pkgs.values())
    return 1 if ours_bad else (2 if ext_bad else 0)


if __name__ == '__main__':
    sys.exit(main())
