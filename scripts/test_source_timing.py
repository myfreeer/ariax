"""Optional diagnostic records retain strict integrity and unchanged timing gates."""
import copy
import unittest

import ci


class SourceTimingTests(unittest.TestCase):
    def report(self):
        return {'samples': 160, 'bursts': 1,
                'operations': {'changeUri': {'calls': 1, 'p50Us': 100250, 'p99Us': 100250, 'maxUs': 100250}},
                'sourceMutationTiming': {'version': 1, 'capacity': 125, 'calls': 1, 'samples': [
                    {'ordinal': 0, 'sampleIndex': 99, 'burst': 1,
                     'clientStartedUnixNs': 1791000000000000000,
                     'backendStartedUnixNs': 1791000000000100000,
                     'roundTripNs': 100250123, 'backendNs': 90000001, 'outsideBackendNs': 10250122}]}}

    def test_admission_uses_distinct_positions_and_stage_schema(self):
        report = self.report()
        report['admissionTiming'] = report.pop('sourceMutationTiming')
        report['operations']['addUri'] = report['operations'].pop('changeUri')
        row = report['admissionTiming']['samples'][0]
        row['sampleIndex'] = 159
        row['stageOffsetsNs'] = list(range(1, 14))
        ci.validate_admission_timing(report)
        with self.assertRaises(RuntimeError):
            ci.validate_latencies(report['operations'], 50000)
        for index, stages in [(99, list(range(13))), (159, list(range(6)))]:
            bad = copy.deepcopy(report)
            bad['admissionTiming']['samples'][0].update(sampleIndex=index, stageOffsetsNs=stages)
            with self.assertRaises(RuntimeError):
                ci.validate_admission_timing(bad)

    def test_valid_diagnostics_do_not_make_a_timing_failure_pass(self):
        report = self.report()
        ci.validate_source_timing(report)
        with self.assertRaisesRegex(RuntimeError, 'p99Us'):
            ci.validate_latencies(report['operations'], 50000)
        ci.validate_source_timing({})  # Historical reports remain readable.

    def test_missing_extra_corrupt_and_misattributed_rows_reject(self):
        for mutate in (
                lambda r: r['sourceMutationTiming'].update(samples=[]),
                lambda r: r['sourceMutationTiming']['samples'].append(copy.deepcopy(r['sourceMutationTiming']['samples'][0])),
                lambda r: r['sourceMutationTiming'].update(calls=True),
                lambda r: r.update(samples=161),
                lambda r: r['operations']['changeUri'].update(maxUs=1)):
            report = self.report(); mutate(report)
            with self.assertRaises(RuntimeError):
                ci.validate_source_timing(report)
        for key, value in [('ordinal', True), ('sampleIndex', 100), ('burst', 2),
                           ('clientStartedUnixNs', 0), ('backendStartedUnixNs', -1),
                           ('backendNs', 100250124), ('outsideBackendNs', 0), ('roundTripNs', '100250123')]:
            report = self.report(); report['sourceMutationTiming']['samples'][0][key] = value
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                ci.validate_source_timing(report)

    def test_source_stages_are_complete_monotonic_and_bounded(self):
        report = self.report()
        report['sourceMutationTiming']['samples'][0]['stageOffsetsNs'] = [1, 2, 3, 4, 5, 90000001]
        ci.validate_source_timing(report)
        for stages in (None, [], [1, 2, 3, 4, 5], [2, 1, 3, 4, 5, 6],
                       [1, 2, 3, 4, 5, 90000002], [True, 2, 3, 4, 5, 6]):
            bad = copy.deepcopy(report)
            bad['sourceMutationTiming']['samples'][0]['stageOffsetsNs'] = stages
            with self.subTest(stages=stages), self.assertRaises(RuntimeError):
                ci.validate_source_timing(bad)
        report['samples'] = 320
        report['sourceMutationTiming']['calls'] = 2
        second = copy.deepcopy(report['sourceMutationTiming']['samples'][0])
        second.update(ordinal=1, sampleIndex=259)
        del second['stageOffsetsNs']
        report['sourceMutationTiming']['samples'].append(second)
        report['operations']['changeUri']['calls'] = 2
        with self.assertRaisesRegex(RuntimeError, 'missing source stages'):
            ci.validate_source_timing(report)

    def test_cross_language_fixture_and_rounding(self):
        report = self.report()
        report['samples'] = 320
        report['sourceMutationTiming']['calls'] = 2
        second = copy.deepcopy(report['sourceMutationTiming']['samples'][0])
        second.update(ordinal=1, sampleIndex=259)
        report['sourceMutationTiming']['samples'].append(second)
        report['operations']['changeUri']['calls'] = 2
        ci.validate_source_timing(report)


if __name__ == '__main__':
    unittest.main()
