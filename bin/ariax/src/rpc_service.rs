use crate::startup::{RpcTransport, StartupOptions};
use ariax_engine::{
    HttpControlBackend, HttpRpcTransportError, RpcAuthPolicy, RpcDispatcher, RpcStdioEof,
};
use serde_json::{Value, json};
use std::io::Read as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

pub(crate) fn read_configuration(startup: &StartupOptions) -> Result<Option<Value>, String> {
    if startup.config_file.is_none()
        && startup.url_rules_file.is_none()
        && startup.scheduling.is_empty()
    {
        return Ok(None);
    }
    let read = |path: &std::path::Path| -> Result<String, String> {
        let mut text = String::new();
        std::fs::File::open(path)
            .and_then(|file| {
                file.take((ariax_engine::MAX_HTTP_RPC_REQUEST_BYTES + 1) as u64)
                    .read_to_string(&mut text)
            })
            .map_err(|_| "cannot read bounded UTF-8 configuration".to_owned())?;
        if text.len() > ariax_engine::MAX_HTTP_RPC_REQUEST_BYTES {
            return Err("configuration exceeds the input limit".to_owned());
        }
        Ok(text)
    };
    let mut text = startup
        .config_file
        .as_deref()
        .map(read)
        .transpose()?
        .unwrap_or_default();
    for (name, value) in &startup.scheduling {
        use std::fmt::Write as _;
        writeln!(text, "\n{name}={value}").expect("configuration text");
    }
    let rules = startup.url_rules_file.as_deref().map(read).transpose()?;
    if text
        .len()
        .saturating_add(rules.as_ref().map_or(0, String::len))
        > ariax_engine::MAX_HTTP_RPC_REQUEST_BYTES
    {
        return Err("combined configuration exceeds the input limit".to_owned());
    }
    Ok(Some(match rules {
        Some(rules) => json!([text, {"urlRules":rules}]),
        None => json!([text]),
    }))
}

pub(crate) async fn serve(
    backend: Arc<HttpControlBackend>,
    auth: RpcAuthPolicy,
    bind: Option<SocketAddr>,
    transport: RpcTransport,
    startup: &StartupOptions,
) -> Result<(), String> {
    let listener = match bind {
        Some(bind) => Some(
            tokio::net::TcpListener::bind(bind)
                .await
                .map_err(|_| "cannot bind loopback RPC listener".to_owned())?,
        ),
        None => None,
    };
    let dispatcher = Arc::new(
        RpcDispatcher::new(backend.clone(), auth).with_compatibility(startup.compatibility),
    );
    let (stop, _) = tokio::sync::watch::channel(false);
    let mut transports = tokio::task::JoinSet::new();
    if let Some(listener) = listener {
        eprintln!(
            "ariax: RPC listening on {}",
            listener.local_addr().map_err(|error| error.to_string())?
        );
        let dispatcher = dispatcher.clone();
        let mut stopped = stop.subscribe();
        transports.spawn(async move {
            let shutdown = async move { crate::wait_for_rpc_shutdown(&mut stopped).await };
            let result = if transport.websocket() {
                ariax_engine::serve_loopback_websocket_listener_until(
                    listener, dispatcher, shutdown,
                )
                .await
            } else {
                ariax_engine::serve_loopback_http_listener_until(listener, dispatcher, shutdown)
                    .await
            };
            (false, result)
        });
    }
    if transport.has_stdio() {
        let dispatcher = dispatcher.clone();
        let options = startup.stdio;
        let mut stopped = stop.subscribe();
        transports.spawn(async move {
            let result = ariax_engine::run_stdio_until(
                dispatcher,
                tokio::io::stdin(),
                tokio::io::stdout(),
                options,
                async move { crate::wait_for_rpc_shutdown(&mut stopped).await },
            )
            .await;
            (true, result)
        });
    }
    let progress_plane = backend.plane();
    let mut progress = tokio::spawn(async move {
        loop {
            let result = progress_plane.lock().await.poll_once();
            if let Err(error) = result
                && !matches!(error, ariax_engine::HttpControlError::Busy)
            {
                return Err::<(), _>(format!("control progress failed: {error}"));
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });
    let mut progress_finished = false;
    let mut shutdown = backend.shutdown_receiver();
    let mut result = loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => break signal.map_err(|error| error.to_string()),
            signal = crate::wait_for_rpc_shutdown(&mut shutdown) => break signal.map_err(|error| error.to_string()),
            joined = &mut progress => {
                progress_finished = true;
                break joined.map_err(|error| error.to_string()).and_then(|result| result);
            }
            joined = transports.join_next(), if !transports.is_empty() => {
                match joined {
                    Some(Ok((true, Ok(())))) if startup.stdio_eof != RpcStdioEof::Shutdown => {},
                    Some(Ok((_, Ok(())))) => break Ok(()),
                    Some(Ok((_, Err(error)))) => break Err(error.to_string()),
                    Some(Err(error)) => break Err(error.to_string()),
                    None => {},
                }
            }
        }
    };
    stop.send_replace(true);
    let drained = tokio::time::timeout(
        ariax_engine::DEFAULT_HTTP_RPC_SHUTDOWN_TIMEOUT + Duration::from_secs(1),
        async {
            while let Some(joined) = transports.join_next().await {
                match joined {
                    Ok((_, Ok(()))) => {}
                    Ok((_, Err(HttpRpcTransportError::Io(error))))
                        if error.kind() == std::io::ErrorKind::BrokenPipe => {}
                    Ok((_, Err(error))) => {
                        if result.is_ok() {
                            result = Err(error.to_string());
                        }
                    }
                    Err(error) => {
                        if result.is_ok() {
                            result = Err(error.to_string());
                        }
                    }
                }
            }
        },
    )
    .await;
    if drained.is_err() {
        transports.abort_all();
        while transports.join_next().await.is_some() {}
        result = Err("RPC transport drain exceeded its deadline".to_owned());
    }
    if !progress_finished {
        progress.abort();
        let _ = progress.await;
    }
    drop(dispatcher);
    result
}
