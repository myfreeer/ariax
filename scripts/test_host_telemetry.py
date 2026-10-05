"""Telemetry is bounded evidence, including unavailable and failed observations."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import ci
import host_telemetry as ht


def stat(group=123, started=42):
    fields = ['0'] * 40
    for index, value in {0: 'S', 2: group, 11: 12, 12: 4, 17: 3, 19: started, 39: 5}.items():
        fields[index] = str(value)
    return '123 (name with ) parentheses) ' + ' '.join(fields)


class ParserTests(unittest.TestCase):
    def test_counters_and_process_positions(self):
        cpu = ht.cpu_stat('cpu 1 2 3 4 5 6 7 8 9 10\nprocs_running 2\nprocs_blocked 1\n')
        self.assertEqual(cpu['ticks']['steal'], 8)
        self.assertEqual(cpu['ticks']['guestNice'], 10)
        self.assertEqual((cpu['running'], cpu['blocked']), (2, 1))
        self.assertEqual(ht.loadavg('0.00 1.00 2.00 2/100 123')['tasks'], 100)
        self.assertEqual(ht.counters('read_bytes: 12\nwrite_bytes: 3'), {'read_bytes': 12, 'write_bytes': 3})
        self.assertEqual(ht.pressure('some avg10=0.00 avg60=0.00 avg300=0.00 total=9'), {'some': 9})
        self.assertEqual(ht.cpu_max('max 100000'), {'quotaUs': None, 'periodUs': 100000})
        self.assertEqual(ht.cpu_max('200000 100000')['quotaUs'], 200000)
        self.assertEqual(ht.process_stat(stat()), {'group': 123, 'state': 'S', 'userTicks': 12,
                         'systemTicks': 4, 'threads': 3, 'startTicks': 42, 'ioWaitTicks': 5})

    def test_malformed_or_truncated_counters_reject(self):
        cases = [(ht.cpu_stat, 'cpu 1 2'), (ht.cpu_stat, 'cpu 1\ncpu 2'),
                 (ht.loadavg, 'NaN 0 0 1/2 3'), (ht.loadavg, '0 0 0 3/2 3'),
                 (ht.loadavg, '0 0 0 1/2 -1'), (ht.counters, 'a 1\na: 2'),
                 (ht.counters, 'a -1'), (ht.counters, ''),
                 (ht.pressure, 'some total=1 total=2'), (ht.pressure, 'other total=2'),
                 (ht.pressure, 'some avg10=nan avg60=0 avg300=0 total=1'),
                 (ht.cpu_max, '0 100'), (ht.cpu_max, 'max 0'),
                 (ht.process_stat, '123 (truncated) S'), (ht.process_stat, '123 no comm')]
        for parser, value in cases:
            with self.subTest(parser=parser.__name__, value=value), self.assertRaises((ValueError, KeyError)):
                parser(value)

    def test_paths_and_bounded_reads(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.assertEqual(ht.cgroup_directory('0::/job', root), root.resolve() / 'job')
            for membership in ('0::/../escape', '0::relative', '1:cpu:/job', '0::/a\n0::/b'):
                with self.subTest(membership=membership), self.assertRaises(ValueError):
                    ht.cgroup_directory(membership, root)
            path = root / 'counter'
            path.write_bytes(b'x' * (ht.READ_LIMIT + 1))
            with self.assertRaises(ValueError):
                ht.read_text(path)
            path.write_bytes(b'\xff')
            with self.assertRaises(ValueError):
                ht.read_text(path)


class SamplerTests(unittest.TestCase):
    def test_missing_metrics_are_explicit_and_sample_and_byte_caps_hold(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            sampler = ht.Sampler(root / 'samples', proc=root, max_samples=1)
            sampler.capture(123)
            sampler.capture(123)
            result = sampler.close()
            row = json.loads((root / 'samples').read_text())
            self.assertIn('unavailable', row['cpu'])
            self.assertIn('unavailable', row['cgroupV2'])
            self.assertGreater(row['unixNs'], 0)
            self.assertEqual(result['samples'], 1)
            self.assertEqual(result['droppedSamples'], 1)
            self.assertEqual(result['bytes'], (root / 'samples').stat().st_size)
            sampler = ht.Sampler(root / 'bytes', proc=root, max_bytes=1)
            sampler.capture(123)
            result = sampler.close()
            self.assertEqual(result['samples'], 0)
            self.assertEqual(result['writeError'], 'ByteLimitReached')
            for limits in ({'max_samples': 0}, {'max_samples': ht.MAX_SAMPLES + 1}, {'max_bytes': 0}):
                with self.assertRaises(ValueError):
                    ht.Sampler(root / 'invalid', **limits)

    def test_cgroup_and_process_group_observations_and_pid_reuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for directory in ('self', '123', '456', 'cg/job'):
                (root / directory).mkdir(parents=True)
            (root / 'self/cgroup').write_text('0::/job')
            (root / 'cg/job/cpu.stat').write_text('usage_usec 20\nnr_throttled 3\nthrottled_usec 4')
            (root / 'cg/job/cpu.max').write_text('100000 100000')
            (root / 'cg/job/cpu.pressure').write_text('some avg10=0 avg60=0 avg300=0 total=7')
            (root / '123/stat').write_text(stat())
            (root / '123/io').write_text('read_bytes: 10')
            (root / '456/stat').write_text(stat(group=456))
            sampler = ht.Sampler(root / 'out', proc=root, cgroup_root=root / 'cg')
            sampler.capture(123)
            rows = sampler.processes(123, sampler.discovery_ns + 1)
            self.assertFalse(rows['discovery']['performed'])
            self.assertEqual([row['pid'] for row in rows['members']], [123])
            self.assertEqual(rows['members'][0]['io']['value']['read_bytes'], 10)
            (root / '123/stat').write_text(stat(started=43))
            row = sampler.processes(123, sampler.discovery_ns + 2)['members'][0]
            self.assertEqual(row['stat'], {'unavailable': 'ProcessIdentityChanged'})
            self.assertIn('unavailable', row['io'])
            sampler.close()
            row = json.loads((root / 'out').read_text())
            self.assertEqual(row['cgroupV2']['cpuStat']['value']['nr_throttled'], 3)

    def test_process_discovery_caps_and_unreadable_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for pid in range(4):
                path = root / str(pid)
                path.mkdir()
                (path / 'stat').write_text(stat())
            (root / '5').mkdir()
            sampler = ht.Sampler(root / 'out', proc=root)
            with mock.patch.object(ht, 'MAX_PROCESSES', 1):
                rows = sampler.processes(123, 1)
            self.assertTrue(rows['discovery']['groupTruncated'])
            self.assertEqual(rows['discovery']['unreadable'], 1)
            self.assertEqual(len(rows['members']), 1)
            with mock.patch.object(ht, 'MAX_SCAN', 1):
                rows = sampler.processes(123, 1_000_000_001)
            self.assertTrue(rows['discovery']['scanTruncated'])
            self.assertEqual(rows['discovery']['scanned'], 1)
            sampler.close()

    def test_write_failure_is_explicit_and_stops_further_collection(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            sampler = ht.Sampler(root / 'out', proc=root)
            sampler.output.close()
            sampler.output = mock.Mock()
            sampler.output.write.side_effect = OSError('full')
            sampler.capture(123)
            sampler.capture(123)
            result = sampler.close()
            self.assertEqual(result['writeError'], 'OSError')
            self.assertEqual(result['samples'], 0)
            self.assertEqual(result['droppedSamples'], 1)

    def test_failed_benchmark_retains_telemetry_and_sampler_failure_cannot_block_cleanup(self):
        for capture_error in (None, RuntimeError('sampler failed')):
            with self.subTest(capture_error=capture_error), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                binary = root / 'benchmark'
                binary.write_bytes(b'fixture')
                process = mock.Mock(returncode=1, pid=123)
                process.poll.return_value = 1

                def sampler_factory(path):
                    path.write_text('{"unixNs":123}\n')
                    sampler = mock.Mock()
                    sampler.capture.side_effect = capture_error
                    sampler.close.return_value = {'samples': 1}
                    return sampler

                with mock.patch.object(ci, 'compiler_processes', return_value=[]), \
                        mock.patch.object(ci.subprocess, 'Popen', return_value=process), \
                        mock.patch.object(ci.os, 'killpg') as kill, \
                        mock.patch.object(ci.host_telemetry, 'Sampler', side_effect=sampler_factory):
                    with self.assertRaises(RuntimeError):
                        ci.measure_scenario(mock.Mock(directory=root, env={}), binary, 'mixed-bt', True)
                    kill.assert_called_once()
                    process.wait.assert_called_once_with(timeout=10)
                record = json.loads((root / 'mixed-bt/run.json').read_text())
                self.assertFalse(record['passed'])
                self.assertEqual(record['hostTelemetry']['samples'], 1)
                self.assertIn('host-load.jsonlSha256', record)
                if capture_error:
                    self.assertEqual(record['hostTelemetryError'], 'RuntimeError')

    def test_successful_benchmark_retains_telemetry(self):
        from test_burst_timing import BurstTimingTests
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / 'benchmark'
            binary.write_bytes(b'fixture')
            process = mock.Mock(returncode=0, pid=123)
            process.poll.return_value = 0

            def launch(command, **kwargs):
                kwargs['stdout'].write((json.dumps(BurstTimingTests().report()) + '\n').encode())
                return process

            with mock.patch.object(ci, 'compiler_processes', return_value=[]), \
                    mock.patch.object(ci.subprocess, 'Popen', side_effect=launch), \
                    mock.patch.object(ci.os, 'killpg'), \
                    mock.patch.object(ci.host_telemetry, 'Sampler') as sampler:
                sampler.return_value.close.return_value = {'samples': 2}
                ci.measure_scenario(mock.Mock(directory=root, env={}), binary, 'mixed-bt', True)
                self.assertEqual(sampler.return_value.capture.call_count, 2)
            record = json.loads((root / 'mixed-bt/run.json').read_text())
            self.assertTrue(record['passed'])
            self.assertEqual(record['hostTelemetry']['samples'], 2)

    def test_sampling_preserves_compiler_activity_rejection_and_cleanup(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / 'benchmark'
            binary.write_bytes(b'fixture')
            process = mock.Mock(pid=123)
            process.poll.return_value = None
            process.wait.side_effect = [subprocess.TimeoutExpired('fixture', ht.INTERVAL), None]
            with mock.patch.object(ci, 'compiler_processes', side_effect=[[], [{'name': 'rustc', 'pid': 9}]]), \
                    mock.patch.object(ci.subprocess, 'Popen', return_value=process), \
                    mock.patch.object(ci.os, 'killpg') as kill, \
                    mock.patch.object(ci.time, 'monotonic', side_effect=[0, 0, 6, 6, 6]), \
                    mock.patch.object(ci.host_telemetry, 'Sampler') as sampler:
                sampler.return_value.close.return_value = {'samples': 3}
                with self.assertRaisesRegex(RuntimeError, 'compiler activity during benchmark'):
                    ci.measure_scenario(mock.Mock(directory=root, env={}), binary, 'mixed-bt', True)
                kill.assert_called_once()
                self.assertEqual(process.wait.call_args_list[0], mock.call(timeout=ht.INTERVAL))
            record = json.loads((root / 'mixed-bt/run.json').read_text())
            self.assertFalse(record['passed'])
            self.assertEqual(record['compilerProcessesObserved'][0]['elapsedSeconds'], 6)


if __name__ == '__main__':
    unittest.main()
