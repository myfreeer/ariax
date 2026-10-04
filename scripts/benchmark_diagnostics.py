"""Summarize bounded small runs while retaining failures and raw timing gates."""
import statistics

import ci


def summarize(attempts, scenario, *, expected_os="windows"):
    rows, failures = [], []
    for index, attempt in enumerate(attempts, 1):
        identity = attempt.get("attempt", index)
        try:
            ci.require(not attempt.get("error"), attempt.get("error", "driver failed"))
            report = attempt.get("report")
            latency_passed = ci.validate_diagnostic_benchmark(report, scenario, expected_os=expected_os)
            ci.require(attempt.get("exitCode") == (0 if latency_passed else 1),
                       "exit status disagrees with complete diagnostic report")
        except (RuntimeError, TypeError, KeyError, ValueError) as error:
            failures.append({"attempt": identity, "error": str(error),
                             "compilerOverlap": bool(attempt.get("compilerProcessesObserved"))})
            continue
        worst = max(operation["p99Us"] for operation in report["operations"].values())
        rows.append({"attempt": identity, "p99Us": report["p99Us"],
                     "worstOperationP99Us": worst, "scoreUs": max(worst, report["p99Us"]),
                     "elapsedScenarioMs": report["elapsedScenarioMs"],
                     "latencyGatePassed": latency_passed,
                     "compilerOverlap": bool(attempt.get("compilerProcessesObserved"))})
    median = statistics.median(row["scoreUs"] for row in rows) if rows else None
    mad = statistics.median(abs(row["scoreUs"] - median) for row in rows) if rows else None
    cutoff = median + max(3 * 1.4826 * mad, 0.25 * median, 1_000) if len(rows) >= 5 else None
    for row in rows:
        row["timingOutlier"] = cutoff is not None and row["scoreUs"] > cutoff
    retained = [row for row in rows if not row["timingOutlier"]]

    def timings(selected):
        if not selected:
            return None
        return {"runs": len(selected),
                "medianP99Us": statistics.median(row["p99Us"] for row in selected),
                "minP99Us": min(row["p99Us"] for row in selected),
                "maxP99Us": max(row["p99Us"] for row in selected),
                "medianWorstOperationP99Us": statistics.median(row["worstOperationP99Us"] for row in selected)}

    return {"scenario": scenario, "attempts": len(attempts), "validCompleteRuns": len(rows),
            "failedAttempts": failures, "failureCount": len(failures),
            "rawLatencyGateFailures": sum(not row["latencyGatePassed"] for row in rows),
            "compilerOverlapRuns": sum(row["compilerOverlap"] for row in rows),
            "allFunctionalChecksPassed": bool(rows) and not failures,
            "allRawTimingGatesPassed": bool(rows) and all(row["latencyGatePassed"] for row in rows) and not failures,
            "acceptanceEligible": False, "filterMinimumRuns": 5,
            "filter": "score > median + max(3 * 1.4826 * MAD, 0.25 * median, 1000 us)",
            "medianScoreUs": median, "medianAbsoluteDeviationUs": mad,
            "upperCutoffUs": cutoff,
            "excludedTimingAttempts": [row["attempt"] for row in rows if row["timingOutlier"]],
            "rawTimings": timings(rows), "filteredTimings": timings(retained),
            "quietFilteredTimings": timings([row for row in retained if not row["compilerOverlap"]]),
            "runs": rows}
