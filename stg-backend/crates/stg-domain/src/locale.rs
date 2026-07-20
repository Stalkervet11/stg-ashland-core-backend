use serde::{Deserialize, Serialize};

/// Supported locales for internationalization.
///
/// The `Default` implementation returns `en_US` (English).
/// When a requested locale is not available, the system
/// falls back to English automatically.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Locale {
    #[serde(rename = "en-US")]
    #[default]
    EnUs,
    #[serde(rename = "ru-RU")]
    RuRu,
}

impl Locale {
    /// Parse a locale from a BCP-47 language tag string.
    /// Returns `None` for unsupported locales (caller should fall back to default).
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "en-US" | "en" | "en_US" => Some(Self::EnUs),
            "ru-RU" | "ru" | "ru_RU" => Some(Self::RuRu),
            _ => None,
        }
    }

    /// BCP-47 language tag for this locale.
    pub fn as_tag(&self) -> &'static str {
        match self {
            Self::EnUs => "en-US",
            Self::RuRu => "ru-RU",
        }
    }

    /// Human-readable name of this locale (in its own language).
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::EnUs => "English (US)",
            Self::RuRu => "Русский (Россия)",
        }
    }

    /// All supported locales, for UI enumeration.
    pub fn all() -> &'static [Locale] {
        &[Self::EnUs, Self::RuRu]
    }

    /// Fallback chain for this locale. The last entry is always English.
    pub fn fallback_chain(&self) -> Vec<Locale> {
        match self {
            Self::EnUs => vec![Self::EnUs],
            Self::RuRu => vec![Self::RuRu, Self::EnUs],
        }
    }
}

impl std::fmt::Display for Locale {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_tag())
    }
}
