//! Runtime account pool for automatic switching on quota exhaustion.
//!
//! Reads codex-auth-compatible `~/.codex/accounts/registry.json` and selects
//! the best available account based on remaining quota.

use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
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
                    .and_then(|ts| Utc.timestamp_opt(ts, 0).single())
                    .filter(|dt| Utc::now() < *dt);
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

        info!(
            "Loaded account pool: {} candidate accounts",
            entries.len()
        );

        Some(Self {
            codex_home: codex_home.to_path_buf(),
            entries: RwLock::new(entries),
            initial_active_key,
        })
    }

    /// Select the best available account (lowest usage score).
    /// The returned `PoolAccountInfo.account_key` is the unique identifier to
    /// pass to `mark_exhausted()` later.
    pub fn try_next_account(
        &self,
        auth_credentials_store_mode: AuthCredentialsStoreMode,
    ) -> Option<(PoolAccountInfo, CodexAuth)> {
        // Extract candidate data under read lock, then release before doing I/O.
        let (account_key, label, auth_file, best_score) = {
            let entries = self.entries.read().ok()?;
            let now = Utc::now();

            let mut best_idx: Option<usize> = None;
            let mut best_score = f64::MAX;

            for (idx, entry) in entries.iter().enumerate() {
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
            (
                entry.account_key.clone(),
                entry.label.clone(),
                entry.auth_file.clone(),
                best_score,
            )
            // read lock released here
        };

        // File I/O and auth construction happen outside the lock.
        let auth_json_str = match fs::read_to_string(&auth_file) {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to read auth file for '{label}': {e}");
                return None;
            }
        };
        let auth_dot_json: AuthDotJson = match serde_json::from_str(&auth_json_str) {
            Ok(a) => a,
            Err(e) => {
                warn!("Failed to parse auth file for '{label}': {e}");
                return None;
            }
        };

        match CodexAuth::from_auth_dot_json(
            &self.codex_home,
            auth_dot_json,
            auth_credentials_store_mode,
        ) {
            Ok(auth) => {
                info!("Selected pool account '{label}' (usage: {best_score:.1}%)");
                let info = PoolAccountInfo { account_key, label };
                Some((info, auth))
            }
            Err(e) => {
                warn!("Failed to create auth for '{label}': {e}");
                None
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
                    let usage = entry.last_usage.get_or_insert(
                        pool_registry::LastUsage {
                            primary: None,
                            secondary: None,
                            plan_type: None,
                        },
                    );
                    if let Some(pct) = primary_used_pct {
                        let w = usage.primary.get_or_insert(
                            pool_registry::UsageWindow {
                                used_percent: None,
                                window_minutes: Some(300),
                                resets_at: None,
                            },
                        );
                        w.used_percent = Some(pct);
                    }
                    if let Some(pct) = secondary_used_pct {
                        let w = usage.secondary.get_or_insert(
                            pool_registry::UsageWindow {
                                used_percent: None,
                                window_minutes: Some(10080),
                                resets_at: None,
                            },
                        );
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
}
