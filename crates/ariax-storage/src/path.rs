use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use unicode_normalization::UnicodeNormalization;

pub const MAX_SAFE_RELATIVE_BYTES: usize = 64 * 1024;

/// Filesystem naming rules relevant to portable persisted paths.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum PathPlatform {
    Unix = 1,
    Windows = 2,
}

impl PathPlatform {
    #[must_use]
    pub const fn current() -> Self {
        if cfg!(windows) {
            Self::Windows
        } else {
            Self::Unix
        }
    }

    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Unix => "unix",
            Self::Windows => "windows",
        }
    }
}

/// A normalized relative path whose components passed the metadata-path policy.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SafeRelativePath {
    components: Box<[String]>,
    encoded_len: usize,
}

impl SafeRelativePath {
    #[must_use]
    pub fn components(&self) -> impl ExactSizeIterator<Item = &str> {
        self.components.iter().map(String::as_str)
    }

    #[must_use]
    pub const fn encoded_len(&self) -> usize {
        self.encoded_len
    }

    /// Constructs a display path only. Native backends must open relative to a root capability.
    #[must_use]
    pub fn display_path(&self) -> PathBuf {
        self.components.iter().collect()
    }

    #[must_use]
    pub fn canonical_string(&self) -> String {
        self.components.join("/")
    }

    pub(crate) fn collision_key(&self, platform: PathPlatform) -> String {
        let path = self.canonical_string();
        match platform {
            PathPlatform::Unix => path,
            PathPlatform::Windows => path.to_lowercase(),
        }
    }
}

impl fmt::Display for SafeRelativePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.canonical_string())
    }
}

/// Portable lexical validation used before a native backend acquires capabilities.
pub struct SafePathBuilder;

impl SafePathBuilder {
    /// Validates an already-separated metadata component list.
    pub fn from_metadata_components<I, S>(
        components: I,
        platform: PathPlatform,
    ) -> Result<SafeRelativePath, PathValidationError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        build_components(components, platform)
    }

    /// Splits an `out`/`index-out`-style user path on either separator.
    pub fn from_user_path(
        value: &str,
        platform: PathPlatform,
    ) -> Result<SafeRelativePath, PathValidationError> {
        reject_absolute_or_prefixed(value)?;
        let components = value
            .split(['/', '\\'])
            .filter(|component| !component.is_empty());
        build_components(components, platform)
    }

    /// Combines a relative `dir` with either explicit `out` components or metadata components.
    pub fn combine(
        dir: Option<&Path>,
        out: Option<&str>,
        metadata_components: &[String],
        platform: PathPlatform,
    ) -> Result<SafeRelativePath, PathValidationError> {
        let mut components = Vec::new();
        if let Some(dir) = dir {
            let dir = dir.to_str().ok_or(PathValidationError::NonUtf8Input)?;
            let path = Self::from_user_path(dir, platform)?;
            components.extend(path.components().map(str::to_owned));
        }
        if let Some(out) = out {
            let path = Self::from_user_path(out, platform)?;
            components.extend(path.components().map(str::to_owned));
        } else {
            let path = Self::from_metadata_components(metadata_components, platform)?;
            components.extend(path.components().map(str::to_owned));
        }
        build_components(components, platform)
    }
}

/// Why a relative metadata path failed portable validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PathValidationError {
    EmptyPath,
    AbsolutePath,
    WindowsPrefix,
    NonUtf8Input,
    EmptyComponent,
    DotComponent,
    SeparatorInComponent,
    ColonInComponent,
    ControlCharacter,
    BidirectionalControl,
    ReservedWindowsName,
    TrailingWindowsDotOrSpace,
    TooLong,
}

impl PathValidationError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::EmptyPath => "empty_path",
            Self::AbsolutePath => "absolute_path",
            Self::WindowsPrefix => "windows_prefix",
            Self::NonUtf8Input => "non_utf8_input",
            Self::EmptyComponent => "empty_component",
            Self::DotComponent => "dot_component",
            Self::SeparatorInComponent => "separator_in_component",
            Self::ColonInComponent => "colon_in_component",
            Self::ControlCharacter => "control_character",
            Self::BidirectionalControl => "bidirectional_control",
            Self::ReservedWindowsName => "reserved_windows_name",
            Self::TrailingWindowsDotOrSpace => "trailing_windows_dot_or_space",
            Self::TooLong => "too_long",
        }
    }
}

pub const ALL_PATH_VALIDATION_ERRORS: [PathValidationError; 13] = [
    PathValidationError::EmptyPath,
    PathValidationError::AbsolutePath,
    PathValidationError::WindowsPrefix,
    PathValidationError::NonUtf8Input,
    PathValidationError::EmptyComponent,
    PathValidationError::DotComponent,
    PathValidationError::SeparatorInComponent,
    PathValidationError::ColonInComponent,
    PathValidationError::ControlCharacter,
    PathValidationError::BidirectionalControl,
    PathValidationError::ReservedWindowsName,
    PathValidationError::TrailingWindowsDotOrSpace,
    PathValidationError::TooLong,
];

impl fmt::Display for PathValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyPath => "safe relative path is empty",
            Self::AbsolutePath => "absolute paths are forbidden",
            Self::WindowsPrefix => "Windows drive and UNC prefixes are forbidden",
            Self::NonUtf8Input => "path option is not valid UTF-8",
            Self::EmptyComponent => "metadata path contains an empty component",
            Self::DotComponent => "metadata path contains '.' or '..'",
            Self::SeparatorInComponent => "metadata component contains a path separator",
            Self::ColonInComponent => "metadata component contains ':'",
            Self::ControlCharacter => "metadata component contains NUL or a control character",
            Self::BidirectionalControl => {
                "metadata component contains a bidirectional override or isolate"
            }
            Self::ReservedWindowsName => "metadata component uses a reserved Windows name",
            Self::TrailingWindowsDotOrSpace => "Windows metadata component ends in a dot or space",
            Self::TooLong => "safe relative path exceeds the 64 KiB encoded limit",
        })
    }
}

impl Error for PathValidationError {}

fn build_components<I, S>(
    components: I,
    platform: PathPlatform,
) -> Result<SafeRelativePath, PathValidationError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut normalized = Vec::new();
    let mut encoded_len = 0_usize;
    for component in components {
        let component = component.as_ref();
        if component.is_empty() {
            return Err(PathValidationError::EmptyComponent);
        }
        let component = component.nfc().collect::<String>();
        validate_component(&component, platform)?;
        if !normalized.is_empty() {
            encoded_len = encoded_len
                .checked_add(1)
                .ok_or(PathValidationError::TooLong)?;
        }
        encoded_len = encoded_len
            .checked_add(component.len())
            .ok_or(PathValidationError::TooLong)?;
        if encoded_len > MAX_SAFE_RELATIVE_BYTES {
            return Err(PathValidationError::TooLong);
        }
        normalized.push(component);
    }
    if normalized.is_empty() {
        return Err(PathValidationError::EmptyPath);
    }
    Ok(SafeRelativePath {
        components: normalized.into_boxed_slice(),
        encoded_len,
    })
}

fn validate_component(component: &str, platform: PathPlatform) -> Result<(), PathValidationError> {
    if matches!(component, "." | "..") {
        return Err(PathValidationError::DotComponent);
    }
    if component.contains(['/', '\\']) {
        return Err(PathValidationError::SeparatorInComponent);
    }
    if component.contains(':') {
        return Err(PathValidationError::ColonInComponent);
    }
    if component.chars().any(char::is_control) {
        return Err(PathValidationError::ControlCharacter);
    }
    if component.chars().any(is_bidirectional_control) {
        return Err(PathValidationError::BidirectionalControl);
    }
    if is_reserved_windows_name(component) {
        return Err(PathValidationError::ReservedWindowsName);
    }
    if platform == PathPlatform::Windows && component.ends_with(['.', ' ']) {
        return Err(PathValidationError::TrailingWindowsDotOrSpace);
    }
    Ok(())
}

fn reject_absolute_or_prefixed(value: &str) -> Result<(), PathValidationError> {
    if value.starts_with('/') {
        return Err(PathValidationError::AbsolutePath);
    }
    if value.starts_with('\\') {
        return Err(PathValidationError::WindowsPrefix);
    }
    let bytes = value.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        return Err(PathValidationError::WindowsPrefix);
    }
    Ok(())
}

fn is_bidirectional_control(character: char) -> bool {
    matches!(character, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
}

fn is_reserved_windows_name(component: &str) -> bool {
    let stem = component
        .split('.')
        .next()
        .unwrap_or(component)
        .to_uppercase();
    matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || numbered_reserved_name(&stem, "COM")
        || numbered_reserved_name(&stem, "LPT")
}

fn numbered_reserved_name(stem: &str, prefix: &str) -> bool {
    let Some(suffix) = stem.strip_prefix(prefix) else {
        return false;
    };
    matches!(
        suffix,
        "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
    )
}

#[cfg(test)]
mod tests {
    use super::{PathPlatform, PathValidationError, SafePathBuilder};
    use std::path::Path;

    #[test]
    fn metadata_components_are_normalized_to_nfc() {
        let path = SafePathBuilder::from_metadata_components(
            ["folder", "e\u{301}.txt"],
            PathPlatform::Unix,
        )
        .expect("valid path");
        assert_eq!(path.canonical_string(), "folder/é.txt");
        assert_eq!(path.encoded_len(), "folder/é.txt".len());
    }

    #[test]
    fn user_paths_split_both_separators_and_ignore_repeats() {
        let path = SafePathBuilder::from_user_path("images\\2026//release.iso", PathPlatform::Unix)
            .expect("valid user path");
        assert_eq!(
            path.components().collect::<Vec<_>>(),
            ["images", "2026", "release.iso"]
        );
    }

    #[test]
    fn dir_and_out_override_metadata_deterministically() {
        let path = SafePathBuilder::combine(
            Some(Path::new("downloads/images")),
            Some("renamed.iso"),
            &["metadata.iso".to_owned()],
            PathPlatform::Unix,
        )
        .expect("combined path");
        assert_eq!(path.canonical_string(), "downloads/images/renamed.iso");
    }

    #[test]
    fn traversal_absolute_prefix_and_component_separators_are_rejected() {
        let user_cases = [
            ("/etc/passwd", PathValidationError::AbsolutePath),
            ("\\\\server\\share", PathValidationError::WindowsPrefix),
            ("C:\\Windows", PathValidationError::WindowsPrefix),
            ("../escape", PathValidationError::DotComponent),
            ("a/./b", PathValidationError::DotComponent),
        ];
        for (value, expected) in user_cases {
            assert_eq!(
                SafePathBuilder::from_user_path(value, PathPlatform::Unix),
                Err(expected),
                "case {value:?}"
            );
        }
        assert_eq!(
            SafePathBuilder::from_metadata_components(["a/b"], PathPlatform::Unix),
            Err(PathValidationError::SeparatorInComponent)
        );
    }

    #[test]
    fn portable_spoofing_and_windows_reserved_names_are_rejected() {
        for value in [
            "has:stream",
            "line\nfeed",
            "right\u{202e}left",
            "CON",
            "nul.log",
            "COM9.txt",
            "LPT¹",
        ] {
            assert!(
                SafePathBuilder::from_metadata_components([value], PathPlatform::Unix).is_err(),
                "case {value:?}"
            );
        }
        assert_eq!(
            SafePathBuilder::from_metadata_components(["name."], PathPlatform::Windows),
            Err(PathValidationError::TrailingWindowsDotOrSpace)
        );
        assert!(SafePathBuilder::from_metadata_components(["name."], PathPlatform::Unix).is_ok());
    }

    #[test]
    fn encoded_relative_path_is_bounded() {
        let oversized = "x".repeat(super::MAX_SAFE_RELATIVE_BYTES + 1);
        assert_eq!(
            SafePathBuilder::from_metadata_components([oversized], PathPlatform::Unix),
            Err(PathValidationError::TooLong)
        );
    }
}
