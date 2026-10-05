"""Bounded Linux benchmark telemetry; missing counters never mean zero load."""
import json
import os
from pathlib import Path, PurePosixPath
import re
import time

INTERVAL = 0.25
MAX_SAMPLES = 512
MAX_BYTES = 4 * 1024 * 1024
READ_LIMIT = 16 * 1024
MAX_PROCESSES = 16
MAX_SCAN = 4096


def read_text(path):
    with path.open('rb') as source:
        data = source.read(READ_LIMIT + 1)
    if len(data) > READ_LIMIT:
        raise ValueError('counter file exceeds bound')
    return data.decode('ascii')


def numbers(fields):
    values = [int(field) for field in fields]
    if any(value < 0 for value in values):
        raise ValueError('negative counter')
    return values


def cpu_stat(text):
    rows = {}
    for line in text.splitlines():
        row = line.split()
        if not row:
            continue
        if row[0] in rows:
            raise ValueError('duplicate CPU stat row')
        rows[row[0]] = row[1:]
    ticks = numbers(rows['cpu'])
    if not 8 <= len(ticks) <= 10:
        raise ValueError('unexpected CPU counter count')
    if any(len(rows[key]) != 1 for key in ('procs_running', 'procs_blocked')):
        raise ValueError('invalid process counter')
    return {'ticks': dict(zip(('user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq', 'steal',
                              'guest', 'guestNice'), ticks)),
            'running': numbers(rows['procs_running'])[0], 'blocked': numbers(rows['procs_blocked'])[0]}


def loadavg(text):
    fields = text.split()
    if len(fields) != 5:
        raise ValueError('invalid load average')
    # Preserve decimal strings; consumers decide how to interpret averages.
    for field in fields[:3]:
        if not re.fullmatch(r'[0-9]+(?:\.[0-9]+)?', field):
            raise ValueError('invalid load average')
    running, total = numbers(fields[3].split('/'))
    numbers(fields[4:])
    if running > total:
        raise ValueError('invalid runnable count')
    return {'averages': fields[:3], 'runnable': running, 'tasks': total}


def counters(text):
    result = {}
    for line in text.splitlines():
        key, value = line.split()
        key = key.rstrip(':')
        if key in result:
            raise ValueError('duplicate counter')
        result[key] = numbers([value])[0]
    if not result:
        raise ValueError('empty counters')
    return result


def pressure(text):
    result = {}
    for line in text.splitlines():
        kind, *fields = line.split()
        if kind not in ('some', 'full') or kind in result:
            raise ValueError('invalid pressure kind')
        values = {}
        for field in fields:
            key, value = field.split('=', 1)
            if key in values:
                raise ValueError('duplicate pressure field')
            values[key] = value
        if set(values) != {'avg10', 'avg60', 'avg300', 'total'} or any(
                not re.fullmatch(r'[0-9]+(?:\.[0-9]+)?', values[key])
                for key in ('avg10', 'avg60', 'avg300')):
            raise ValueError('invalid pressure fields')
        result[kind] = numbers([values['total']])[0]
    if not result:
        raise ValueError('empty pressure counters')
    return result


def cpu_max(text):
    quota, period = text.split()
    period = numbers([period])[0]
    quota = None if quota == 'max' else numbers([quota])[0]
    if period == 0 or quota == 0:
        raise ValueError('invalid CPU quota')
    return {'quotaUs': quota, 'periodUs': period}


def process_stat(text):
    # The parenthesized comm field can contain spaces and parentheses.
    head, separator, tail = text.rpartition(') ')
    if not separator or '(' not in head:
        raise ValueError('invalid process stat')
    fields = tail.split()
    if len(fields) < 40:
        raise ValueError('truncated process stat')
    group, user, system, threads, started, blocked = numbers(
        [fields[index] for index in (2, 11, 12, 17, 19, 39)])
    return {'group': group, 'state': fields[0], 'userTicks': user, 'systemTicks': system,
            'threads': threads, 'startTicks': started, 'ioWaitTicks': blocked}


def cgroup_directory(text, root):
    matches = [line[3:] for line in text.splitlines() if line.startswith('0::')]
    if len(matches) != 1:
        raise ValueError('cgroup v2 membership unavailable')
    path = PurePosixPath(matches[0])
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('unsafe cgroup membership')
    directory = root.joinpath(*path.parts[1:]).resolve()
    if not directory.is_relative_to(root.resolve()):
        raise ValueError('cgroup escapes mount')
    return directory


class Sampler:
    def __init__(self, output, *, proc=Path('/proc'), cgroup_root=Path('/sys/fs/cgroup'),
                 max_samples=MAX_SAMPLES, max_bytes=MAX_BYTES):
        if not 0 < max_samples <= MAX_SAMPLES or not 0 < max_bytes <= MAX_BYTES:
            raise ValueError('invalid telemetry bounds')
        self.proc = proc
        self.clock_ticks = os.sysconf('SC_CLK_TCK')
        self.output = Path(output).open('xb')
        self.max_samples, self.max_bytes = max_samples, max_bytes
        self.count = self.bytes = self.dropped = self.errors = 0
        self.max_cost_us = self.cpu_ns = 0
        self.watched = {}
        self.discovery_ns = None
        self.cgroup = None
        self.cgroup_error = None
        self.write_error = None
        try:
            self.cgroup = cgroup_directory(read_text(proc / 'self/cgroup'), cgroup_root)
        except (OSError, ValueError) as error:
            self.cgroup_error = type(error).__name__

    def metric(self, path, parser):
        try:
            return {'value': parser(read_text(path))}
        except (OSError, ValueError, KeyError, IndexError) as error:
            self.errors += 1
            return {'unavailable': type(error).__name__}

    def processes(self, group, now):
        discovery = {'performed': False}
        if self.discovery_ns is None or now - self.discovery_ns >= 1_000_000_000:
            self.discovery_ns = now
            self.watched = {}
            discovery = {'performed': True, 'scanned': 0, 'scanTruncated': False,
                         'groupTruncated': False, 'unreadable': 0}
            try:
                for path in self.proc.iterdir():
                    if not path.name.isdecimal():
                        continue
                    if discovery['scanned'] >= MAX_SCAN:
                        discovery['scanTruncated'] = True
                        break
                    discovery['scanned'] += 1
                    try:
                        stat = process_stat(read_text(path / 'stat'))
                    except (OSError, ValueError, IndexError):
                        discovery['unreadable'] += 1
                        continue
                    if stat['group'] == group:
                        if len(self.watched) == MAX_PROCESSES:
                            discovery['groupTruncated'] = True
                        else:
                            self.watched[int(path.name)] = stat['startTicks']
            except OSError as error:
                discovery['unavailable'] = type(error).__name__
        rows = []
        for pid, start in self.watched.items():
            value = self.metric(self.proc / str(pid) / 'stat', process_stat)
            if 'value' in value and (value['value']['group'] != group or value['value']['startTicks'] != start):
                value = {'unavailable': 'ProcessIdentityChanged'}
            rows.append({'pid': pid, 'stat': value,
                         'io': self.metric(self.proc / str(pid) / 'io', counters)
                         if 'value' in value else {'unavailable': 'ProcessUnavailable'}})
        return {'discovery': discovery, 'members': rows}

    def capture(self, group):
        if self.count >= self.max_samples or self.write_error:
            self.dropped += 1
            return
        started = time.monotonic_ns()
        cpu_started = time.process_time_ns()
        row = {'monotonicNs': started, 'unixNs': time.time_ns(),
               'cpu': self.metric(self.proc / 'stat', cpu_stat),
               'load': self.metric(self.proc / 'loadavg', loadavg),
               'pressureTotalUs': {kind: self.metric(self.proc / 'pressure' / kind, pressure)
                                   for kind in ('cpu', 'io', 'memory')},
               'processes': self.processes(group, started)}
        if self.cgroup is None:
            row['cgroupV2'] = {'unavailable': self.cgroup_error}
        else:
            row['cgroupV2'] = {'cpuStat': self.metric(self.cgroup / 'cpu.stat', counters),
                               'cpuMax': self.metric(self.cgroup / 'cpu.max', cpu_max),
                               'cpuPressureTotalUs': self.metric(self.cgroup / 'cpu.pressure', pressure)}
        row['readDurationUs'] = (time.monotonic_ns() - started) // 1_000
        encoded = (json.dumps(row, separators=(',', ':')) + '\n').encode()
        try:
            if self.bytes + len(encoded) > self.max_bytes:
                self.dropped += 1
                self.write_error = 'ByteLimitReached'
            else:
                self.output.write(encoded)
                self.output.flush()
                self.count += 1
                self.bytes += len(encoded)
        except OSError as error:
            self.write_error = type(error).__name__
        self.cpu_ns += time.process_time_ns() - cpu_started
        self.max_cost_us = max(self.max_cost_us, (time.monotonic_ns() - started) // 1_000)

    def close(self):
        try:
            self.output.close()
        except OSError as error:
            self.write_error = type(error).__name__
        return {'version': 1, 'intervalMs': int(INTERVAL * 1000), 'maxSamples': self.max_samples,
                'maxBytes': self.max_bytes, 'samples': self.count, 'bytes': self.bytes,
                'droppedSamples': self.dropped, 'metricErrors': self.errors,
                'writeError': self.write_error, 'maxCollectionUs': self.max_cost_us,
                'samplerCpuNs': self.cpu_ns, 'clockTicksPerSecond': self.clock_ticks,
                'processDiscoveryIntervalMs': 1000, 'maxProcesses': MAX_PROCESSES,
                'scope': 'Sampled host and benchmark process-group counters; not overload attribution'}
