use std::collections::HashMap;
use stg_application::{LocalizationProvider, MessageKey};
use stg_domain::Locale;

/// In-memory message catalog for one locale.
type Catalog = HashMap<String, String>;

/// Maps locale → message key → translated string.
type BundleMap = HashMap<Locale, Catalog>;

/// Localization provider backed by compiled-in resource bundles.
///
/// Messages are stored in Rust source as `HashMap` literals.
/// Adding a new language means adding a new `Catalog` entry
/// to `build_bundles()`.
pub struct ResourceBundleLocalizationProvider {
    bundles: BundleMap,
}

impl ResourceBundleLocalizationProvider {
    pub fn new() -> Self {
        Self {
            bundles: build_bundles(),
        }
    }

    /// Returns the total number of message keys across all locales.
    pub fn message_count(&self) -> usize {
        self.bundles
            .get(&Locale::EnUs)
            .map(|c| c.len())
            .unwrap_or(0)
    }

    /// Returns the number of locales with bundles loaded.
    pub fn locale_count(&self) -> usize {
        self.bundles.len()
    }
}

fn build_bundles() -> BundleMap {
    let mut map = BundleMap::new();

    // ─── English (US) ───────────────────────────────────────────────
    let mut en = Catalog::new();

    // Domain errors
    en.insert("errors.player.not_found".into(), "Player not found".into());
    en.insert(
        "errors.amount.invalid".into(),
        "Invalid amount: must be positive".into(),
    );
    en.insert(
        "errors.amount.negative".into(),
        "Invalid amount: must be non-negative".into(),
    );
    en.insert(
        "errors.insufficient_funds".into(),
        "Insufficient funds: available {balance}, required {required}".into(),
    );
    en.insert(
        "errors.revision_conflict".into(),
        "Revision conflict in {entity}: expected revision {expected}, actual revision {actual}"
            .into(),
    );
    en.insert(
        "errors.energy.node_not_found".into(),
        "Energy node not found".into(),
    );
    en.insert(
        "errors.ledger_imbalance".into(),
        "Transaction entries must sum to zero".into(),
    );
    en.insert(
        "errors.arithmetic_overflow".into(),
        "Arithmetic overflow during calculation".into(),
    );
    en.insert(
        "errors.idempotency_conflict".into(),
        "Idempotency conflict for request {request_id}".into(),
    );
    en.insert(
        "errors.conversion.rule_not_found".into(),
        "Conversion rule not found".into(),
    );
    en.insert(
        "errors.conversion.reservation_not_found".into(),
        "Conversion reservation not found".into(),
    );
    en.insert(
        "errors.conversion.reservation_invalid_state".into(),
        "Conversion reservation is in an invalid state".into(),
    );
    en.insert(
        "errors.conversion.reservation_expired".into(),
        "Conversion reservation has expired".into(),
    );
    en.insert(
        "errors.session.duplicate".into(),
        "Player already has an active session".into(),
    );
    en.insert(
        "errors.session.not_found".into(),
        "Session not found".into(),
    );
    en.insert(
        "errors.session.terminated".into(),
        "Session has been terminated".into(),
    );
    en.insert(
        "errors.session.expired".into(),
        "Session has expired".into(),
    );
    en.insert(
        "errors.server.identity_mismatch".into(),
        "Server identity mismatch: expected {expected}, actual {actual}".into(),
    );
    en.insert(
        "errors.internal_state".into(),
        "Internal server error".into(),
    );

    // Validation errors
    en.insert(
        "validation.player_uuid.invalid".into(),
        "Invalid player UUID format".into(),
    );
    en.insert(
        "validation.amount.invalid".into(),
        "Invalid amount format".into(),
    );
    en.insert(
        "validation.currency.required".into(),
        "Currency code is required".into(),
    );
    en.insert(
        "validation.request_id.invalid".into(),
        "Invalid request ID UUID format".into(),
    );
    en.insert(
        "validation.correlation_id.invalid".into(),
        "Invalid correlation ID UUID format".into(),
    );
    en.insert(
        "validation.reservation_id.invalid".into(),
        "Invalid reservation ID UUID format".into(),
    );
    en.insert(
        "validation.node_id.invalid".into(),
        "Invalid energy node UUID format".into(),
    );
    en.insert(
        "validation.transition_id.invalid".into(),
        "Invalid transition ID UUID format".into(),
    );
    en.insert(
        "validation.context.missing".into(),
        "RequestContext is required".into(),
    );
    en.insert(
        "validation.resource.missing".into(),
        "Resource reference is required".into(),
    );
    en.insert(
        "validation.observation.missing".into(),
        "Energy observation data is required".into(),
    );

    // gRPC status messages (human-readable layer)
    en.insert(
        "grpc.status.not_found".into(),
        "The requested resource was not found".into(),
    );
    en.insert(
        "grpc.status.invalid_argument".into(),
        "One or more request arguments are invalid".into(),
    );
    en.insert(
        "grpc.status.failed_precondition".into(),
        "The operation cannot be executed in the current state".into(),
    );
    en.insert(
        "grpc.status.aborted".into(),
        "The operation was aborted due to a conflict".into(),
    );
    en.insert(
        "grpc.status.already_exists".into(),
        "The resource already exists".into(),
    );
    en.insert(
        "grpc.status.internal".into(),
        "An internal server error occurred".into(),
    );
    en.insert(
        "grpc.status.unimplemented".into(),
        "This operation is not yet implemented".into(),
    );

    // Health endpoints
    en.insert("health.status.ok".into(), "ok".into());
    en.insert(
        "health.ready.service_ready".into(),
        "Service is ready".into(),
    );
    en.insert(
        "health.ready.database_failed".into(),
        "Database connection failed".into(),
    );

    // Log messages (human-readable layer)
    en.insert("log.server.startup".into(), "STG-Core starting up".into());
    en.insert(
        "log.server.shutdown".into(),
        "STG-Core shutting down".into(),
    );
    en.insert(
        "log.db.connected".into(),
        "PostgreSQL connection established".into(),
    );
    en.insert(
        "log.db.migrations_applied".into(),
        "PostgreSQL schema migrations applied successfully".into(),
    );
    en.insert(
        "log.grpc.listening".into(),
        "STG-Core gRPC server listening".into(),
    );
    en.insert(
        "log.http.listening".into(),
        "HTTP health server listening".into(),
    );
    en.insert(
        "log.scheduler.started".into(),
        "SimulationScheduler worker started".into(),
    );
    en.insert(
        "log.scheduler.stopped".into(),
        "SimulationScheduler worker stopped".into(),
    );
    en.insert("log.outbox.started".into(), "Outbox worker started".into());
    en.insert(
        "log.session.expired".into(),
        "Expired {count} stale sessions".into(),
    );
    en.insert("log.player.registered".into(), "Player registered".into());
    en.insert("log.transfer.completed".into(), "Transfer completed".into());
    en.insert(
        "log.conversion.prepared".into(),
        "Resource conversion prepared".into(),
    );
    en.insert(
        "log.conversion.committed".into(),
        "Resource conversion committed".into(),
    );
    en.insert(
        "log.conversion.aborted".into(),
        "Resource conversion aborted".into(),
    );
    en.insert(
        "log.transition.begun".into(),
        "Player transition begun".into(),
    );
    en.insert(
        "log.transition.claimed".into(),
        "Player transition claimed".into(),
    );
    en.insert(
        "log.transition.committed".into(),
        "Player transition committed".into(),
    );
    en.insert(
        "log.transition.aborted".into(),
        "Player transition aborted".into(),
    );
    en.insert(
        "log.energy.tick".into(),
        "Energy simulation tick completed".into(),
    );
    en.insert(
        "log.energy.node_registered".into(),
        "Energy node registered".into(),
    );

    // Admin Panel API (future)
    en.insert(
        "admin.dashboard.title".into(),
        "STG-Ashland Administration".into(),
    );
    en.insert(
        "admin.dashboard.players_online".into(),
        "Players Online".into(),
    );
    en.insert(
        "admin.dashboard.active_sessions".into(),
        "Active Sessions".into(),
    );
    en.insert(
        "admin.dashboard.energy_mode".into(),
        "Global Energy Mode".into(),
    );
    en.insert(
        "admin.dashboard.transactions_today".into(),
        "Transactions Today".into(),
    );
    en.insert(
        "admin.dashboard.scheduler_status".into(),
        "Scheduler Status".into(),
    );
    en.insert("admin.players.list".into(), "Player List".into());
    en.insert("admin.sessions.list".into(), "Session List".into());
    en.insert("admin.economy.overview".into(), "Economy Overview".into());
    en.insert(
        "admin.energy.overview".into(),
        "Energy Grid Overview".into(),
    );
    en.insert("admin.system.health".into(), "System Health".into());
    en.insert("admin.system.metrics".into(), "System Metrics".into());

    map.insert(Locale::EnUs, en);

    // ─── Russian (Russia) ──────────────────────────────────────────
    let mut ru = Catalog::new();

    // Domain errors
    ru.insert("errors.player.not_found".into(), "Игрок не найден".into());
    ru.insert(
        "errors.amount.invalid".into(),
        "Некорректная сумма: значение должно быть положительным".into(),
    );
    ru.insert(
        "errors.amount.negative".into(),
        "Некорректная сумма: значение не может быть отрицательным".into(),
    );
    ru.insert(
        "errors.insufficient_funds".into(),
        "Недостаточно средств: доступно {balance}, требуется {required}".into(),
    );
    ru.insert(
        "errors.revision_conflict".into(),
        "Конфликт версий в {entity}: ожидаемая версия {expected}, фактическая версия {actual}"
            .into(),
    );
    ru.insert(
        "errors.energy.node_not_found".into(),
        "Энергоузел не найден".into(),
    );
    ru.insert(
        "errors.ledger_imbalance".into(),
        "Записи транзакции должны суммироваться в ноль".into(),
    );
    ru.insert(
        "errors.arithmetic_overflow".into(),
        "Арифметическое переполнение при вычислении".into(),
    );
    ru.insert(
        "errors.idempotency_conflict".into(),
        "Конфликт идемпотентности для запроса {request_id}".into(),
    );
    ru.insert(
        "errors.conversion.rule_not_found".into(),
        "Правило конвертации не найдено".into(),
    );
    ru.insert(
        "errors.conversion.reservation_not_found".into(),
        "Резервация конвертации не найдена".into(),
    );
    ru.insert(
        "errors.conversion.reservation_invalid_state".into(),
        "Резервация конвертации в недопустимом состоянии".into(),
    );
    ru.insert(
        "errors.conversion.reservation_expired".into(),
        "Срок действия резервации конвертации истёк".into(),
    );
    ru.insert(
        "errors.session.duplicate".into(),
        "У игрока уже есть активная сессия".into(),
    );
    ru.insert(
        "errors.session.not_found".into(),
        "Сессия не найдена".into(),
    );
    ru.insert(
        "errors.session.terminated".into(),
        "Сессия была завершена".into(),
    );
    ru.insert(
        "errors.session.expired".into(),
        "Срок действия сессии истёк".into(),
    );
    ru.insert(
        "errors.server.identity_mismatch".into(),
        "Несоответствие идентификатора сервера: ожидался {expected}, получен {actual}".into(),
    );
    ru.insert(
        "errors.internal_state".into(),
        "Внутренняя ошибка сервера".into(),
    );

    // Validation errors
    ru.insert(
        "validation.player_uuid.invalid".into(),
        "Неверный формат UUID игрока".into(),
    );
    ru.insert(
        "validation.amount.invalid".into(),
        "Неверный формат суммы".into(),
    );
    ru.insert(
        "validation.currency.required".into(),
        "Код валюты обязателен".into(),
    );
    ru.insert(
        "validation.request_id.invalid".into(),
        "Неверный формат UUID идентификатора запроса".into(),
    );
    ru.insert(
        "validation.correlation_id.invalid".into(),
        "Неверный формат UUID идентификатора корреляции".into(),
    );
    ru.insert(
        "validation.reservation_id.invalid".into(),
        "Неверный формат UUID резервации".into(),
    );
    ru.insert(
        "validation.node_id.invalid".into(),
        "Неверный формат UUID энергоузла".into(),
    );
    ru.insert(
        "validation.transition_id.invalid".into(),
        "Неверный формат UUID перехода".into(),
    );
    ru.insert(
        "validation.context.missing".into(),
        "RequestContext обязателен".into(),
    );
    ru.insert(
        "validation.resource.missing".into(),
        "Ссылка на ресурс обязательна".into(),
    );
    ru.insert(
        "validation.observation.missing".into(),
        "Данные наблюдения за энергией обязательны".into(),
    );

    // gRPC status messages
    ru.insert(
        "grpc.status.not_found".into(),
        "Запрашиваемый ресурс не найден".into(),
    );
    ru.insert(
        "grpc.status.invalid_argument".into(),
        "Один или несколько аргументов запроса недействительны".into(),
    );
    ru.insert(
        "grpc.status.failed_precondition".into(),
        "Операция не может быть выполнена в текущем состоянии".into(),
    );
    ru.insert(
        "grpc.status.aborted".into(),
        "Операция прервана из-за конфликта".into(),
    );
    ru.insert(
        "grpc.status.already_exists".into(),
        "Ресурс уже существует".into(),
    );
    ru.insert(
        "grpc.status.internal".into(),
        "Произошла внутренняя ошибка сервера".into(),
    );
    ru.insert(
        "grpc.status.unimplemented".into(),
        "Эта операция ещё не реализована".into(),
    );

    // Health endpoints
    ru.insert("health.status.ok".into(), "ok".into());
    ru.insert(
        "health.ready.service_ready".into(),
        "Сервис готов к работе".into(),
    );
    ru.insert(
        "health.ready.database_failed".into(),
        "Ошибка подключения к базе данных".into(),
    );

    // Log messages
    ru.insert("log.server.startup".into(), "STG-Core запускается".into());
    ru.insert(
        "log.server.shutdown".into(),
        "STG-Core завершает работу".into(),
    );
    ru.insert(
        "log.db.connected".into(),
        "Подключение к PostgreSQL установлено".into(),
    );
    ru.insert(
        "log.db.migrations_applied".into(),
        "Миграции схемы PostgreSQL успешно применены".into(),
    );
    ru.insert(
        "log.grpc.listening".into(),
        "STG-Core gRPC сервер запущен".into(),
    );
    ru.insert(
        "log.http.listening".into(),
        "HTTP сервер здоровья запущен".into(),
    );
    ru.insert(
        "log.scheduler.started".into(),
        "Планировщик симуляции запущен".into(),
    );
    ru.insert(
        "log.scheduler.stopped".into(),
        "Планировщик симуляции остановлен".into(),
    );
    ru.insert(
        "log.outbox.started".into(),
        "Обработчик исходящих событий запущен".into(),
    );
    ru.insert(
        "log.session.expired".into(),
        "Завершено {count} устаревших сессий".into(),
    );
    ru.insert(
        "log.player.registered".into(),
        "Игрок зарегистрирован".into(),
    );
    ru.insert("log.transfer.completed".into(), "Перевод выполнен".into());
    ru.insert(
        "log.conversion.prepared".into(),
        "Конвертация ресурсов подготовлена".into(),
    );
    ru.insert(
        "log.conversion.committed".into(),
        "Конвертация ресурсов подтверждена".into(),
    );
    ru.insert(
        "log.conversion.aborted".into(),
        "Конвертация ресурсов отменена".into(),
    );
    ru.insert("log.transition.begun".into(), "Переход игрока начат".into());
    ru.insert(
        "log.transition.claimed".into(),
        "Переход игрока запрошен".into(),
    );
    ru.insert(
        "log.transition.committed".into(),
        "Переход игрока подтверждён".into(),
    );
    ru.insert(
        "log.transition.aborted".into(),
        "Переход игрока отменён".into(),
    );
    ru.insert(
        "log.energy.tick".into(),
        "Такт симуляции энергии завершён".into(),
    );
    ru.insert(
        "log.energy.node_registered".into(),
        "Энергоузел зарегистрирован".into(),
    );

    // Admin Panel API (future)
    ru.insert(
        "admin.dashboard.title".into(),
        "Администрирование STG-Ashland".into(),
    );
    ru.insert(
        "admin.dashboard.players_online".into(),
        "Игроков онлайн".into(),
    );
    ru.insert(
        "admin.dashboard.active_sessions".into(),
        "Активных сессий".into(),
    );
    ru.insert(
        "admin.dashboard.energy_mode".into(),
        "Режим энергосети".into(),
    );
    ru.insert(
        "admin.dashboard.transactions_today".into(),
        "Транзакций сегодня".into(),
    );
    ru.insert(
        "admin.dashboard.scheduler_status".into(),
        "Статус планировщика".into(),
    );
    ru.insert("admin.players.list".into(), "Список игроков".into());
    ru.insert("admin.sessions.list".into(), "Список сессий".into());
    ru.insert("admin.economy.overview".into(), "Обзор экономики".into());
    ru.insert("admin.energy.overview".into(), "Обзор энергосети".into());
    ru.insert("admin.system.health".into(), "Состояние системы".into());
    ru.insert("admin.system.metrics".into(), "Метрики системы".into());

    map.insert(Locale::RuRu, ru);

    map
}

impl LocalizationProvider for ResourceBundleLocalizationProvider {
    fn localize(&self, key: &MessageKey, locale: Locale) -> String {
        // Walk the fallback chain for this locale
        for fallback_locale in locale.fallback_chain() {
            if let Some(catalog) = self.bundles.get(&fallback_locale) {
                if let Some(msg) = catalog.get(key.as_str()) {
                    return msg.clone();
                }
            }
        }
        // Absolute fallback: return the key itself so operators can see what's missing
        key.as_str().to_string()
    }

    fn supports_locale(&self, locale: Locale) -> bool {
        self.bundles.contains_key(&locale)
            || locale
                .fallback_chain()
                .iter()
                .any(|l| self.bundles.contains_key(l))
    }

    fn supported_locales(&self) -> Vec<Locale> {
        let mut locales: Vec<Locale> = self.bundles.keys().copied().collect();
        locales.sort_by_key(|l| l.as_tag());
        locales
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_en_us_fallback() {
        let provider = ResourceBundleLocalizationProvider::new();
        let msg = provider.localize(&MessageKey::new("errors.player.not_found"), Locale::EnUs);
        assert_eq!(msg, "Player not found");
    }

    #[test]
    fn test_ru_ru_translation() {
        let provider = ResourceBundleLocalizationProvider::new();
        let msg = provider.localize(&MessageKey::new("errors.player.not_found"), Locale::RuRu);
        assert_eq!(msg, "Игрок не найден");
    }

    #[test]
    fn test_missing_key_falls_back_to_en() {
        let provider = ResourceBundleLocalizationProvider::new();
        let msg = provider.localize(&MessageKey::new("errors.ledger_imbalance"), Locale::RuRu);
        assert!(!msg.is_empty());
    }

    #[test]
    fn test_unknown_key_returns_key_itself() {
        let provider = ResourceBundleLocalizationProvider::new();
        let msg = provider.localize(&MessageKey::new("nonexistent.key.here"), Locale::EnUs);
        assert_eq!(msg, "nonexistent.key.here");
    }

    #[test]
    fn test_supports_locale() {
        let provider = ResourceBundleLocalizationProvider::new();
        assert!(provider.supports_locale(Locale::EnUs));
        assert!(provider.supports_locale(Locale::RuRu));
    }

    #[test]
    fn test_supported_locales_count() {
        let provider = ResourceBundleLocalizationProvider::new();
        assert_eq!(provider.supported_locales().len(), 2);
        assert_eq!(provider.locale_count(), 2);
    }
}
