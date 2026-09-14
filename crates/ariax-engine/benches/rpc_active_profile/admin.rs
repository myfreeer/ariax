//! Separate administrative measurements; no active-range claim is made here.

use super::*;

const TASKS: usize = 128;

async fn timed(
    backend: &Arc<HttpControlBackend>,
    method: &'static str,
    params: Value,
    targets: usize,
    urgent: Option<(&'static str, Value)>,
) -> Result<(Value, Value)> {
    let start = Instant::now();
    let call = backend.call(method, params);
    let operation = tokio::spawn(async move {
        let result = call.await;
        (result, start.elapsed())
    });
    let mut queries = Vec::new();
    let mut query_completions_while_pending = 0;
    let mut urgent = urgent;
    let mut urgent_latency = None;
    let mut bursts = 0;
    let mut max_burst = Duration::ZERO;
    let query_method = if method == "aria2.purgeDownloadResult" {
        "aria2.tellStopped"
    } else {
        "aria2.tellWaiting"
    };
    while !operation.is_finished() {
        let burst = Instant::now();
        let mut calls = 0;
        while !operation.is_finished()
            && calls < 1000
            && burst.elapsed() < Duration::from_millis(BURST_LAUNCH_MS)
        {
            let sent = Instant::now();
            let states = backend
                .call(query_method, json!([0, 1000, ["gid", "status"]]))
                .await?;
            queries.push(sent.elapsed());
            if !operation.is_finished() {
                query_completions_while_pending += 1;
            }
            calls += 1;
            let states = states.as_array().ok_or("missing concurrent task list")?;
            let progressed = match method {
                "aria2.unpauseAll" => states.iter().any(|task| task["status"] == "waiting"),
                "aria2.pauseAll" => {
                    states
                        .iter()
                        .filter(|task| task["status"] == "paused")
                        .count()
                        > states.len().saturating_sub(targets)
                }
                "aria2.purgeDownloadResult" => states.len() < targets,
                _ => false,
            };
            if calls < 1000
                && burst.elapsed() < Duration::from_millis(BURST_LAUNCH_MS)
                && progressed
                && !operation.is_finished()
                && let Some((method, gid)) = urgent.take()
            {
                let sent = Instant::now();
                if backend.call(method, json!([gid])).await? != gid {
                    return Err("administrative urgent acknowledgement changed".into());
                }
                urgent_latency = Some(sent.elapsed());
                calls += 1;
            }
        }
        max_burst = max_burst.max(burst.elapsed());
        if max_burst > Duration::from_millis(500) {
            return Err("administrative query burst exceeded 500 ms".into());
        }
        bursts += 1;
        if !operation.is_finished() {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    let (result, duration) = operation.await?;
    let result = result?;
    if urgent.is_some() {
        return Err("administrative operation finished before its ordered urgent probe".into());
    }
    let mut samples = std::collections::BTreeMap::new();
    if !queries.is_empty() {
        samples.insert(query_method, queries);
    }
    let query_report = latency_report(&mut samples);
    if samples
        .values()
        .any(|samples| samples[(samples.len() * 99).div_ceil(100) - 1] > Duration::from_millis(50))
        || urgent_latency.is_some_and(|elapsed| elapsed > Duration::from_millis(50))
    {
        return Err(format!(
            "administrative query or urgent latency exceeded 50 ms: {query_report}"
        )
        .into());
    }
    let report = json!({"method":method,"targets":targets,"completed":true,"durationUs":duration.as_micros(),
        "queries":query_report,"queryCompletionsWhilePending":query_completions_while_pending,
        "urgentCalls":usize::from(urgent_latency.is_some()),"urgentUs":urgent_latency.map(|elapsed| elapsed.as_micros()),
        "urgentAfterObservedProgress":urgent_latency.is_some(),
        "queryBursts":bursts,"maxQueryBurstUs":max_burst.as_micros(),"cooldownMs":250});
    tokio::time::sleep(Duration::from_millis(250)).await;
    Ok((result, report))
}

async fn states(backend: &HttpControlBackend) -> Result<(usize, usize)> {
    let value = backend
        .call("aria2.tellWaiting", json!([0, 1000, ["gid", "status"]]))
        .await?;
    let tasks = value.as_array().ok_or("missing task list")?;
    Ok((
        tasks
            .iter()
            .filter(|task| task["status"] == "waiting")
            .count(),
        tasks
            .iter()
            .filter(|task| task["status"] == "paused")
            .count(),
    ))
}

pub(super) async fn measure() -> Result<()> {
    let scenario_started = Instant::now();
    let root = Root::new()?;
    let resources = HttpProcessResources::for_profile(RuntimeProfile::Concurrency)?;
    let (mut plane, _) = build_control_plane(&root, &resources, TASKS + 1)?;
    let export_path = root.0.join("export.json");
    plane.configure_session_export(SessionExportConfig {
        path: export_path.clone(),
        format: SessionFormat::Json,
        interval: None,
    })?;
    let backend = Arc::new(HttpControlBackend::new(plane));
    backend.start_control_runtime()?;
    let document = json!({"tasks":(0..TASKS).map(|index| json!({"uris":[format!("http://example.test/admin-{index}.bin")],"options":{"pause":true}})).collect::<Vec<_>>()});
    let (gids, mut imported) = timed(
        &backend,
        "ariax.importSession",
        json!([document]),
        TASKS,
        None,
    )
    .await?;
    let gids = gids
        .as_array()
        .filter(|gids| gids.len() == TASKS)
        .ok_or("incomplete import")?
        .clone();
    if states(&backend).await? != (0, TASKS) {
        return Err("import did not publish every paused member".into());
    }
    imported["publishedTasks"] = json!(TASKS);
    let last = gids.last().unwrap().clone();
    let (_, mut resumed) = timed(
        &backend,
        "aria2.unpauseAll",
        json!([]),
        TASKS,
        Some(("aria2.pause", last.clone())),
    )
    .await?;
    if states(&backend).await? != (TASKS - 1, 1) {
        return Err("later pause did not supersede unfinished resume-all".into());
    }
    resumed["resumedTasks"] = json!(TASKS - 1);
    resumed["laterPausePreserved"] = json!(true);
    let (_, mut paused) = timed(
        &backend,
        "aria2.pauseAll",
        json!([]),
        TASKS - 1,
        Some(("aria2.unpause", last.clone())),
    )
    .await?;
    if states(&backend).await? != (1, TASKS - 1) {
        return Err("pause-all did not preserve its captured membership".into());
    }
    paused["pausedTasks"] = json!(TASKS - 1);
    paused["laterResumePreserved"] = json!(true);
    let (document, mut exported) =
        timed(&backend, "ariax.exportSession", json!([]), TASKS, None).await?;
    if document["tasks"].as_array().map(Vec::len) != Some(TASKS) {
        return Err("incomplete session export".into());
    }
    exported["exportedTasks"] = json!(TASKS);
    exported["bytes"] = json!(serde_json::to_vec(&document)?.len());
    let (saved, mut saved_report) =
        timed(&backend, "aria2.saveSession", json!([]), TASKS, None).await?;
    if saved != "OK" {
        return Err("configured save failed".into());
    }
    let saved: Value = serde_json::from_slice(&std::fs::read(&export_path)?)?;
    if saved["tasks"].as_array().map(Vec::len) != Some(TASKS) {
        return Err("configured save cardinality changed".into());
    }
    saved_report["exportedTasks"] = json!(TASKS);

    // Build real stopped results for purge, respecting the same short bursts.
    let mut removed = 0;
    let mut removal_bursts = 0;
    while removed < TASKS {
        let burst = Instant::now();
        let mut calls = 0;
        while removed < TASKS
            && calls < 1000
            && burst.elapsed() < Duration::from_millis(BURST_LAUNCH_MS)
        {
            if backend.call("aria2.remove", json!([gids[removed]])).await? != gids[removed] {
                return Err("result setup remove failed".into());
            }
            removed += 1;
            calls += 1;
        }
        if burst.elapsed() > Duration::from_millis(500) {
            return Err("result setup burst exceeded 500 ms".into());
        }
        removal_bursts += 1;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let results = backend
        .call("aria2.tellStopped", json!([0, 1000, ["gid"]]))
        .await?;
    if results.as_array().map(Vec::len) != Some(TASKS) {
        return Err("purge setup lacks real stopped results".into());
    }
    let probe = backend
        .call(
            "aria2.addUri",
            json!([["http://example.test/probe.bin"], {"pause":true}]),
        )
        .await?;
    let (_, mut purged) = timed(
        &backend,
        "aria2.purgeDownloadResult",
        json!([]),
        TASKS,
        Some(("aria2.unpause", probe)),
    )
    .await?;
    if backend.call("aria2.tellStopped", json!([0, 1000])).await? != json!([])
        || states(&backend).await? != (1, 0)
    {
        return Err("purge did not delete exactly its stopped members".into());
    }
    purged["deletedTasks"] = json!(TASKS);
    let runtime = backend.control_runtime_metrics();
    if runtime.max_steps > 32 {
        return Err("owner exceeded its step budget".into());
    }
    let budgets = resources.rpc_budgets().snapshot();
    let rss = rss_bytes()?;
    if budgets.bytes > budgets.byte_limit
        || budgets.resident_bytes > budgets.resident_limit
        || rss > resources.profile().limits().resident_target_bytes as u64
    {
        return Err("administrative memory limit exceeded".into());
    }
    let shutdown_started = Instant::now();
    backend.call("aria2.shutdown", json!([])).await?;
    let acknowledgement = shutdown_started.elapsed();
    backend.drain_control_runtime().await?;
    let backend = Arc::try_unwrap(backend).map_err(|_| "retained administrative backend")?;
    let plane = backend
        .try_into_control_plane()
        .map_err(|_| "retained administrative owner")?;
    if !plane.shutdown_async().await?.is_clean() {
        return Err("unclean administrative shutdown".into());
    }
    println!(
        "{}",
        json!({"scenario":"administrative","os":std::env::consts::OS,"arch":std::env::consts::ARCH,"profile":"concurrency",
        "transport":"in-process backend using the production control runtime","activeRanges":0,"complete":true,"passed":true,
        "operations":[imported,resumed,paused,exported,saved_report,purged],"resultSetupRemovals":removed,"resultSetupBursts":removal_bursts,
        "controlRuntime":runtime,"sampledRssBytes":rss,"rpcBytes":budgets.bytes,"residentBytes":budgets.resident_bytes,
        "shutdownAcknowledgementUs":acknowledgement.as_micros(),"shutdownDrainUs":shutdown_started.elapsed().as_micros(),
        "elapsedScenarioMs":scenario_started.elapsed().as_millis()})
    );
    Ok(())
}
