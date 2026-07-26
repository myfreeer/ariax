#![forbid(unsafe_code)]

use std::env;
use std::ffi::OsString;
use std::process::ExitCode;

const HELP: &str = "ariax — experimental bounded downloader\n\nUsage: ariax [--help|--version]\n";

fn main() -> ExitCode {
    run(env::args_os().skip(1))
}

fn run(arguments: impl IntoIterator<Item = OsString>) -> ExitCode {
    let arguments: Vec<_> = arguments.into_iter().collect();
    match arguments.as_slice() {
        [] => {
            print!("{HELP}");
            ExitCode::SUCCESS
        }
        [arg] if arg == "--help" || arg == "-h" => {
            print!("{HELP}");
            ExitCode::SUCCESS
        }
        [arg] if arg == "--version" || arg == "-V" => {
            println!("{} {}", ariax_core::ENGINE_NAME, env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        [arg] => {
            eprintln!("ariax: unknown argument: {}", arg.to_string_lossy());
            ExitCode::from(2)
        }
        _ => {
            eprintln!("ariax: only one bootstrap argument is accepted");
            ExitCode::from(2)
        }
    }
}
