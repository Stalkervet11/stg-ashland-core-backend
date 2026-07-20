use stg_domain::Locale;

/// Key used to look up a localized message.
///
/// Keys follow a dot-notation hierarchy:
/// `category.subcategory.specific_name`
///
/// Examples:
/// - `errors.player.not_found`
/// - `health.status.ok`
/// - `validation.amount.negative`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MessageKey(String);

impl MessageKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for MessageKey {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for MessageKey {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl std::fmt::Display for MessageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Provides localized messages for a given key and locale.
///
/// This is the application-layer port. Infrastructure implements
/// it with actual translation bundles (files, database, etc.).
///
/// The trait is synchronous because message lookup is a pure
/// in-memory operation with no I/O.
pub trait LocalizationProvider: Send + Sync {
    /// Get a localized message by key, falling back to English
    /// if the key is missing in the requested locale.
    fn localize(&self, key: &MessageKey, locale: Locale) -> String;

    /// Check whether a locale is supported by this provider.
    fn supports_locale(&self, locale: Locale) -> bool;

    /// List all supported locales.
    fn supported_locales(&self) -> Vec<Locale>;
}
