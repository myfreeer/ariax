//! CLI spelling normalization; admission remains owned by the typed engine API.

use ariax_config::{ValueType, builtin_registry};
use std::ffi::OsString;

#[derive(Debug, PartialEq)]
pub(super) enum Argument<'a> {
    Option(&'a str, &'a str),
    Positional(&'a str),
}

pub(super) struct Arguments<'a> {
    remaining: std::slice::Iter<'a, OsString>,
    literal: bool,
}

impl<'a> Arguments<'a> {
    pub(super) fn new(values: &'a [OsString]) -> Self {
        Self {
            remaining: values.iter(),
            literal: false,
        }
    }

    pub(super) fn next(&mut self) -> Result<Option<Argument<'a>>, String> {
        let Some(argument) = self.remaining.next() else {
            return Ok(None);
        };
        let text = argument.to_str().ok_or("arguments must be UTF-8")?;
        if !self.literal && text == "--" {
            self.literal = true;
            return self.next();
        }
        if self.literal || !text.starts_with('-') || text == "-" {
            return Ok(Some(Argument::Positional(text)));
        }
        let (name, inline) = if let Some(long) = text.strip_prefix("--") {
            long.split_once('=')
                .map_or((long, None), |(name, value)| (name, Some(value)))
        } else {
            let mut short = text[1..].chars();
            let alias = short.next().ok_or("invalid short option")?;
            let definition = builtin_registry()
                .definitions()
                .iter()
                .find(|definition| definition.short == Some(alias))
                .ok_or("unknown short option")?;
            let suffix = short.as_str();
            (definition.name, (!suffix.is_empty()).then_some(suffix))
        };
        if name.is_empty() {
            return Err("empty option name".into());
        }
        let boolean = name == "pause"
            || builtin_registry()
                .find(name)
                .is_some_and(|definition| definition.value_type == ValueType::Bool);
        let value = match inline {
            Some(value) => value,
            None if boolean => "true",
            None => self.remaining.next().and_then(|value| value.to_str())
                // A dash-leading value is unambiguous in the =VALUE form.
                .filter(|value| !value.starts_with('-'))
                .ok_or("option requires a value; use --NAME=VALUE for a dash-leading value")?,
        };
        Ok(Some(Argument::Option(name, value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aliases_boolean_defaults_and_literal_arguments_are_unambiguous() {
        let values = [
            "-s4",
            "--pause",
            "uri",
            "--pause=false",
            "--",
            "-o",
            "--x=y",
        ]
        .map(OsString::from);
        let mut arguments = Arguments::new(&values);
        for expected in [
            Argument::Option("split", "4"),
            Argument::Option("pause", "true"),
            Argument::Positional("uri"),
            Argument::Option("pause", "false"),
            Argument::Positional("-o"),
            Argument::Positional("--x=y"),
        ] {
            assert_eq!(arguments.next().unwrap(), Some(expected));
        }
        assert_eq!(arguments.next().unwrap(), None);
    }

    #[test]
    fn malformed_options_do_not_echo_values() {
        for values in [
            vec!["--=secret-canary"],
            vec!["-Zsecret-canary"],
            vec!["--out"],
            vec!["--out", "--secret-canary"],
            vec!["--out", "--"],
        ] {
            let values = values.into_iter().map(OsString::from).collect::<Vec<_>>();
            let error = Arguments::new(&values).next().unwrap_err();
            assert!(!error.contains("canary"));
        }
    }
}
