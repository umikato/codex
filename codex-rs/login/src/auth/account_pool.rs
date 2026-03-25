//! Runtime account pool for automatic switching on quota exhaustion.
//!
//! Reads codex-auth-compatible `~/.codex/accounts/registry.json` and selects
//! the best available account based on remaining quota.

use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use codex_backend_openapi_models::models::RateLimitStatusPayload;
use codex_client::build_reqwest_client_with_custom_ca;
use reqwest::header::AUTHORIZATION;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::RwLock;
use tracing::info;
use tracing::warn;

use super::manager::CodexAuth;
use super::pool_registry;
use super::storage::AuthCredentialsStoreMode;
use super::storage::AuthDotJson;

// ─── Public info returned on account switch ───

#[derive(Debug, Clone)]
pub struct PoolAccountInfo {
    /// Unique identifier (e.g. "user-id::account-id").
    pub account_key: String,
    /// Human-readable display label.
    pub label: String,
}

// ─── Runtime state per pool entry ───

#[derive(Debug)]
struct PoolEntry {
    account_key: String,
    /// Human-readable label: alias > email > account_key
    label: String,
    /// Path to {base64(account_key)}.auth.json
    auth_file: PathBuf,
    /// Cached usage from registry (used for candidate scoring)
    last_usage: Option<pool_registry::LastUsage>,
    /// Once set, this account is skipped until the timestamp passes.
    exhausted_until: Option<DateTime<Utc>>,
}

impl PoolEntry {
    /// Score for candidate selection: lower = more quota remaining = better.
    /// Returns the max used_percent across both windows.
    /// Accounts with no usage data score 50.0 (unknown/neutral) rather than
    /// 0.0, so accounts with known low usage are preferred over unknowns.
    fn usage_score(&self) -> f64 {
        let Some(ref usage) = self.last_usage else {
            return 50.0;
        };
        let primary_used = usage
            .primary
            .as_ref()
            .and_then(|w| w.used_percent)
            .unwrap_or(0.0);
        let secondary_used = usage
            .secondary
            .as_ref()
            .and_then(|w| w.used_percent)
            .unwrap_or(0.0);
        primary_used.max(secondary_used)
    }
}

#[derive(Debug, Clone)]
struct CandidateSelection {
    account_key: String,
    label: String,
    auth_file: PathBuf,
    best_score: f64,
    expired_exhaustion: bool,
}

#[derive(Debug)]
enum CandidateRefreshOutcome {
    Available {
        last_usage: Option<pool_registry::LastUsage>,
    },
    StillExhausted {
        last_usage: Option<pool_registry::LastUsage>,
        exhausted_until: DateTime<Utc>,
    },
    RetryWithoutRefresh,
}

const DEFAULT_CHATGPT_USAGE_ENDPOINT: &str = "https://chatgpt.com/backend-api/wham/usage";

// ─── AccountPool manager ───

#[derive(Debug)]
pub struct AccountPool {
    codex_home: PathBuf,
    entries: RwLock<Vec<PoolEntry>>,
    /// The account_key of the initially active account from registry.
    /// Used so the first switch can mark it as exhausted.
    initial_active_key: Option<String>,
}

impl AccountPool {
    /// Load the account pool from `$CODEX_HOME/accounts/registry.json`.
    /// Returns `None` if the file does not exist or contains fewer than 2 accounts.
    pub fn load(codex_home: &Path) -> Option<Self> {
        let registry = pool_registry::load_registry(codex_home);

        // Need at least 2 accounts for pooling to make sense.
        if registry.accounts.len() < 2 {
            return None;
        }

        let accounts_dir = codex_home.join("accounts");
        let initial_active_key = registry.active_account_key.clone();

        // Include ALL accounts (including the currently active one) so that
        // when the active account is exhausted and we switch away, it can
        // later be switched back to once its cooldown expires.
        let entries: Vec<PoolEntry> = registry
            .accounts
            .into_iter()
            .filter(|acct| {
                let encoded = pool_registry::encode_account_key(&acct.account_key);
                let auth_file = accounts_dir.join(format!("{encoded}.auth.json"));
                auth_file.exists()
            })
            .map(|acct| {
                let label = acct.display_label().to_string();
                let encoded = pool_registry::encode_account_key(&acct.account_key);
                let auth_file = accounts_dir.join(format!("{encoded}.auth.json"));
                // Restore exhausted_until from persisted registry data.
                let exhausted_until = acct
                    .exhausted_until
                    .and_then(|ts| Utc.timestamp_opt(ts, 0).single());
                PoolEntry {
                    account_key: acct.account_key,
                    label,
                    auth_file,
                    last_usage: acct.last_usage,
                    exhausted_until,
                }
            })
            .collect();

        if entries.is_empty() {
            return None;
        }

        info!("Loaded account pool: {} candidate accounts", entries.len());

        Some(Self {
            codex_home: codex_home.to_path_buf(),
            entries: RwLock::new(entries),
            initial_active_key,
        })
    }

    /// Select the best available account (lowest usage score).
    /// The returned `PoolAccountInfo.account_key` is the unique identifier to
    /// pass to `mark_exhausted()` later.
    pub async fn try_next_account(
        &self,
        auth_credentials_store_mode: AuthCredentialsStoreMode,
    ) -> Option<(PoolAccountInfo, CodexAuth)> {
        let mut attempted_account_keys = HashSet::new();

        loop {
            let candidate = self.next_candidate_selection(&attempted_account_keys)?;

            if candidate.expired_exhaustion {
                match self.refresh_expired_candidate(&candidate).await {
                    CandidateRefreshOutcome::Available { last_usage } => {
                        self.apply_runtime_snapshot(
                            &candidate.account_key,
                            last_usage.as_ref(),
                            None,
                            true,
                        );
                        continue;
                    }
                    CandidateRefreshOutcome::StillExhausted {
                        last_usage,
                        exhausted_until,
                    } => {
                        self.apply_runtime_snapshot(
                            &candidate.account_key,
                            last_usage.as_ref(),
                            Some(exhausted_until),
                            true,
                        );
                        attempted_account_keys.insert(candidate.account_key);
                        continue;
                    }
                    CandidateRefreshOutcome::RetryWithoutRefresh => {
                        self.apply_runtime_snapshot(&candidate.account_key, None, None, true);
                        continue;
                    }
                }
            }

            let auth_json_str = match fs::read_to_string(&candidate.auth_file) {
                Ok(s) => s,
                Err(e) => {
                    warn!("Failed to read auth file for '{}': {e}", candidate.label);
                    attempted_account_keys.insert(candidate.account_key);
                    continue;
                }
            };
            let auth_dot_json: AuthDotJson = match serde_json::from_str(&auth_json_str) {
                Ok(a) => a,
                Err(e) => {
                    warn!("Failed to parse auth file for '{}': {e}", candidate.label);
                    attempted_account_keys.insert(candidate.account_key);
                    continue;
                }
            };

            match CodexAuth::from_auth_dot_json(
                &self.codex_home,
                auth_dot_json,
                auth_credentials_store_mode,
            ) {
                Ok(auth) => {
                    info!(
                        "Selected pool account '{}' (usage: {:.1}%)",
                        candidate.label, candidate.best_score
                    );
                    let info = PoolAccountInfo {
                        account_key: candidate.account_key,
                        label: candidate.label,
                    };
                    return Some((info, auth));
                }
                Err(e) => {
                    warn!("Failed to create auth for '{}': {e}", candidate.label);
                    attempted_account_keys.insert(candidate.account_key);
                }
            }
        }
    }

    /// Mark an account as exhausted by its unique `account_key`.
    /// Persists the state (usage + exhausted_until) to `registry.json`.
    pub fn mark_exhausted(&self, account_key: &str, until: Option<DateTime<Utc>>) {
        let fallback = Utc::now() + chrono::Duration::minutes(5);
        let until = until.unwrap_or(fallback);

        let persisted_usage = if let Ok(mut entries) = self.entries.write() {
            let mut found_usage = None;
            for entry in entries.iter_mut() {
                if entry.account_key == account_key {
                    info!(
                        "Marking '{}' ({}) as exhausted until {}",
                        entry.label, account_key, until
                    );
                    entry.exhausted_until = Some(until);
                    found_usage = Some(entry.last_usage.clone());
                    break;
                }
            }
            found_usage
        } else {
            None
        };

        // Persist to registry.json outside the lock.
        pool_registry::persist_account_state(
            &self.codex_home,
            account_key,
            persisted_usage.as_ref().and_then(|u| u.as_ref()),
            Some(until.timestamp()),
            true, // set exhausted_until
        );
    }

    /// Get the account_key of the initially active account from the registry.
    /// Used to bootstrap `current_pool_key` so the first switch can mark it exhausted.
    pub fn initial_active_key(&self) -> Option<&str> {
        self.initial_active_key.as_deref()
    }

    /// Update the cached usage score for an account (in-memory only).
    /// Disk persistence happens later when `mark_exhausted()` is called,
    /// which bundles the latest usage together with the exhaustion timestamp.
    pub fn update_usage(
        &self,
        account_key: &str,
        primary_used_pct: Option<f64>,
        secondary_used_pct: Option<f64>,
    ) {
        if let Ok(mut entries) = self.entries.write() {
            for entry in entries.iter_mut() {
                if entry.account_key == account_key {
                    let usage = entry.last_usage.get_or_insert(pool_registry::LastUsage {
                        primary: None,
                        secondary: None,
                        plan_type: None,
                    });
                    if let Some(pct) = primary_used_pct {
                        let w = usage.primary.get_or_insert(pool_registry::UsageWindow {
                            used_percent: None,
                            window_minutes: Some(300),
                            resets_at: None,
                        });
                        w.used_percent = Some(pct);
                    }
                    if let Some(pct) = secondary_used_pct {
                        let w = usage.secondary.get_or_insert(pool_registry::UsageWindow {
                            used_percent: None,
                            window_minutes: Some(10080),
                            resets_at: None,
                        });
                        w.used_percent = Some(pct);
                    }
                    return;
                }
            }
        }
    }

    /// Check whether any account might be available.
    pub fn has_available_accounts(&self) -> bool {
        let entries = match self.entries.read() {
            Ok(e) => e,
            Err(_) => return false,
        };
        let now = Utc::now();
        entries
            .iter()
            .any(|e| e.exhausted_until.is_none_or(|until| now >= until))
    }

    fn next_candidate_selection(
        &self,
        attempted_account_keys: &HashSet<String>,
    ) -> Option<CandidateSelection> {
        let entries = self.entries.read().ok()?;
        let now = Utc::now();

        let mut best_idx: Option<usize> = None;
        let mut best_score = f64::MAX;

        for (idx, entry) in entries.iter().enumerate() {
            if attempted_account_keys.contains(&entry.account_key) {
                continue;
            }
            if let Some(until) = entry.exhausted_until
                && now < until
            {
                continue;
            }
            let score = entry.usage_score();
            if score < best_score {
                best_score = score;
                best_idx = Some(idx);
            }
        }

        let idx = best_idx?;
        let entry = &entries[idx];
        Some(CandidateSelection {
            account_key: entry.account_key.clone(),
            label: entry.label.clone(),
            auth_file: entry.auth_file.clone(),
            best_score,
            expired_exhaustion: entry.exhausted_until.is_some_and(|until| now >= until),
        })
    }

    async fn refresh_expired_candidate(
        &self,
        candidate: &CandidateSelection,
    ) -> CandidateRefreshOutcome {
        let auth_json_str = match fs::read_to_string(&candidate.auth_file) {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    "Failed to read auth file while rechecking '{}': {e}",
                    candidate.label
                );
                return CandidateRefreshOutcome::RetryWithoutRefresh;
            }
        };
        let auth_dot_json: AuthDotJson = match serde_json::from_str(&auth_json_str) {
            Ok(a) => a,
            Err(e) => {
                warn!(
                    "Failed to parse auth file while rechecking '{}': {e}",
                    candidate.label
                );
                return CandidateRefreshOutcome::RetryWithoutRefresh;
            }
        };
        let auth = match CodexAuth::from_auth_dot_json(
            &self.codex_home,
            auth_dot_json,
            AuthCredentialsStoreMode::File,
        ) {
            Ok(auth) => auth,
            Err(e) => {
                warn!(
                    "Failed to construct auth while rechecking '{}': {e}",
                    candidate.label
                );
                return CandidateRefreshOutcome::RetryWithoutRefresh;
            }
        };

        let usage = match fetch_usage_snapshot_for_auth(&auth).await {
            Ok(usage) => usage,
            Err(e) => {
                warn!(
                    "Failed to refresh usage while rechecking '{}': {e}",
                    candidate.label
                );
                return CandidateRefreshOutcome::RetryWithoutRefresh;
            }
        };

        let Some(last_usage) = usage else {
            return CandidateRefreshOutcome::RetryWithoutRefresh;
        };

        let now_ts = Utc::now().timestamp();
        let fallback_ts = (Utc::now() + chrono::Duration::minutes(5)).timestamp();
        if let Some(exhausted_until) =
            pool_registry::compute_exhausted_until_from_usage(&last_usage, now_ts, fallback_ts)
        {
            let until = Utc
                .timestamp_opt(exhausted_until, 0)
                .single()
                .unwrap_or_else(|| Utc::now() + chrono::Duration::minutes(5));
            return CandidateRefreshOutcome::StillExhausted {
                last_usage: Some(last_usage),
                exhausted_until: until,
            };
        }

        CandidateRefreshOutcome::Available {
            last_usage: Some(last_usage),
        }
    }

    fn apply_runtime_snapshot(
        &self,
        account_key: &str,
        last_usage: Option<&pool_registry::LastUsage>,
        exhausted_until: Option<DateTime<Utc>>,
        set_exhausted_until: bool,
    ) {
        if let Ok(mut entries) = self.entries.write() {
            for entry in entries.iter_mut() {
                if entry.account_key == account_key {
                    if let Some(usage) = last_usage {
                        entry.last_usage = Some(usage.clone());
                    }
                    if set_exhausted_until {
                        entry.exhausted_until = exhausted_until;
                    }
                    break;
                }
            }
        }

        pool_registry::persist_account_runtime_snapshot(
            &self.codex_home,
            account_key,
            last_usage,
            exhausted_until.map(|until| until.timestamp()),
            set_exhausted_until,
        );
    }
}

async fn fetch_usage_snapshot_for_auth(
    auth: &CodexAuth,
) -> std::io::Result<Option<pool_registry::LastUsage>> {
    if !auth.is_chatgpt_auth() {
        return Ok(None);
    }

    let token = auth.get_token()?;
    let Some(account_id) = auth.get_account_id() else {
        return Ok(None);
    };

    let client = build_reqwest_client_with_custom_ca(reqwest::Client::builder())
        .map_err(std::io::Error::other)?;
    let response = client
        .get(DEFAULT_CHATGPT_USAGE_ENDPOINT)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header("ChatGPT-Account-Id", account_id)
        .send()
        .await
        .map_err(std::io::Error::other)?;

    let status = response.status();
    let body = response.text().await.map_err(std::io::Error::other)?;
    if !status.is_success() {
        return Err(std::io::Error::other(format!(
            "GET {DEFAULT_CHATGPT_USAGE_ENDPOINT} failed: {status}; body={body}"
        )));
    }

    let payload: RateLimitStatusPayload =
        serde_json::from_str(&body).map_err(std::io::Error::other)?;
    Ok(last_usage_from_usage_payload(payload))
}

fn last_usage_from_usage_payload(
    payload: RateLimitStatusPayload,
) -> Option<pool_registry::LastUsage> {
    let primary = payload
        .rate_limit
        .as_ref()
        .and_then(|details| details.as_ref())
        .and_then(|details| details.primary_window.as_ref())
        .and_then(|window| window.as_ref())
        .map(|window| pool_registry::UsageWindow {
            used_percent: Some(f64::from(window.used_percent)),
            window_minutes: Some(i64::from((window.limit_window_seconds + 59) / 60)),
            resets_at: Some(i64::from(window.reset_at)),
        });
    let secondary = payload
        .rate_limit
        .as_ref()
        .and_then(|details| details.as_ref())
        .and_then(|details| details.secondary_window.as_ref())
        .and_then(|window| window.as_ref())
        .map(|window| pool_registry::UsageWindow {
            used_percent: Some(f64::from(window.used_percent)),
            window_minutes: Some(i64::from((window.limit_window_seconds + 59) / 60)),
            resets_at: Some(i64::from(window.reset_at)),
        });

    if primary.is_none() && secondary.is_none() {
        return None;
    }

    Some(pool_registry::LastUsage {
        primary,
        secondary,
        plan_type: Some(match payload.plan_type {
            codex_backend_openapi_models::models::rate_limit_status_payload::PlanType::Guest => {
                "guest"
            }
            codex_backend_openapi_models::models::rate_limit_status_payload::PlanType::Free => {
                "free"
            }
            codex_backend_openapi_models::models::rate_limit_status_payload::PlanType::Go => "go",
            codex_backend_openapi_models::models::rate_limit_status_payload::PlanType::Plus => {
                "plus"
            }
            codex_backend_openapi_models::models::rate_limit_status_payload::PlanType::Pro => {
                "pro"
            }
            codex_backend_openapi_models::models::rate_limit_status_payload::PlanType::FreeWorkspace => "free_workspace",
            codex_backend_openapi_models::models::rate_limit_status_payload::PlanType::Team => "team",
            codex_backend_openapi_models::models::rate_limit_status_payload::PlanType::Business => "business",
            codex_backend_openapi_models::models::rate_limit_status_payload::PlanType::Education => "education",
            codex_backend_openapi_models::models::rate_limit_status_payload::PlanType::Quorum => "quorum",
            codex_backend_openapi_models::models::rate_limit_status_payload::PlanType::K12 => "k12",
            codex_backend_openapi_models::models::rate_limit_status_payload::PlanType::Enterprise => "enterprise",
            codex_backend_openapi_models::models::rate_limit_status_payload::PlanType::Edu => "edu",
        }
        .to_string()),
    })
}
