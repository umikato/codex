//! Manage `~/.codex/accounts/registry.json` — compatible with codex-auth.
//!
//! Provides CRUD operations for the account pool: login-and-register,
//! import, list, switch, remove.  The on-disk format is intentionally
//! identical to codex-auth's so either tool can manage the same pool.

use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use std::fs;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::path::PathBuf;
use tracing::info;
use tracing::warn;

use super::storage::AuthDotJson;
use crate::token_data::IdTokenInfo;

// ─── On-disk schema (codex-auth compatible) ───

pub const SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Registry {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_account_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_account_activated_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_switch: Option<AutoSwitchConfig>,
    #[serde(default)]
    pub accounts: Vec<AccountRecord>,
}

fn default_schema_version() -> u32 {
    SCHEMA_VERSION
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoSwitchConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_threshold_5h")]
    pub threshold_5h_percent: f64,
    #[serde(default = "default_threshold_weekly")]
    pub threshold_weekly_percent: f64,
}

fn default_threshold_5h() -> f64 {
    10.0
}
fn default_threshold_weekly() -> f64 {
    5.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountRecord {
    pub account_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chatgpt_user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chatgpt_account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default)]
    pub alias: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_mode: Option<String>,
    #[serde(default)]
    pub created_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_usage: Option<LastUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_usage_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastUsage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary: Option<UsageWindow>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secondary: Option<UsageWindow>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageWindow {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used_percent: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_minutes: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resets_at: Option<i64>,
}

impl AccountRecord {
    /// Human-readable display label: alias > email > account_key.
    pub fn display_label(&self) -> &str {
        if !self.alias.is_empty() {
            return &self.alias;
        }
        if let Some(ref email) = self.email {
            return email.as_str();
        }
        &self.account_key
    }
}

// ─── Registry I/O ───

fn accounts_dir(codex_home: &Path) -> PathBuf {
    codex_home.join("accounts")
}

fn registry_path(codex_home: &Path) -> PathBuf {
    accounts_dir(codex_home).join("registry.json")
}

fn account_auth_path(codex_home: &Path, account_key: &str) -> PathBuf {
    accounts_dir(codex_home).join(format!("{account_key}.auth.json"))
}

fn active_auth_path(codex_home: &Path) -> PathBuf {
    codex_home.join("auth.json")
}

pub fn ensure_accounts_dir(codex_home: &Path) -> std::io::Result<()> {
    fs::create_dir_all(accounts_dir(codex_home))
}

pub fn load_registry(codex_home: &Path) -> Registry {
    let path = registry_path(codex_home);
    match fs::read_to_string(&path) {
        Ok(contents) => serde_json::from_str(&contents).unwrap_or_else(|e| {
            warn!("Failed to parse registry.json: {e}");
            default_registry()
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => default_registry(),
        Err(e) => {
            warn!("Failed to read registry.json: {e}");
            default_registry()
        }
    }
}

pub fn save_registry(codex_home: &Path, registry: &Registry) -> std::io::Result<()> {
    ensure_accounts_dir(codex_home)?;
    let path = registry_path(codex_home);
    let json = serde_json::to_string_pretty(registry)?;

    let mut options = fs::OpenOptions::new();
    options.truncate(true).write(true).create(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    let mut file = options.open(&path)?;
    file.write_all(json.as_bytes())?;
    file.flush()?;
    Ok(())
}

fn default_registry() -> Registry {
    Registry {
        schema_version: SCHEMA_VERSION,
        active_account_key: None,
        active_account_activated_at_ms: None,
        auto_switch: None,
        accounts: Vec::new(),
    }
}

// ─── Account key from JWT claims ───

/// Build the account_key from IdTokenInfo: `{user_id}::{account_id}`.
/// Returns `None` if either field is missing.
pub fn account_key_from_id_token(info: &IdTokenInfo) -> Option<String> {
    let user_id = info.chatgpt_user_id.as_deref()?;
    let account_id = info.chatgpt_account_id.as_deref()?;
    Some(format!("{user_id}::{account_id}"))
}

/// Build an `AccountRecord` from an `AuthDotJson` by parsing its JWT.
/// Returns `None` if the auth data lacks the required token fields.
pub fn account_record_from_auth(auth: &AuthDotJson) -> Option<AccountRecord> {
    let tokens = auth.tokens.as_ref()?;
    let id_info = &tokens.id_token;
    let account_key = account_key_from_id_token(id_info)?;

    let plan = id_info.get_chatgpt_plan_type();
    let auth_mode = auth
        .auth_mode
        .as_ref()
        .map(|m| format!("{m:?}").to_lowercase());

    Some(AccountRecord {
        account_key,
        chatgpt_user_id: id_info.chatgpt_user_id.clone(),
        chatgpt_account_id: id_info.chatgpt_account_id.clone(),
        email: id_info.email.as_ref().map(|e| e.to_lowercase()),
        alias: String::new(),
        plan,
        auth_mode,
        created_at: Utc::now().timestamp(),
        last_used_at: None,
        last_usage: None,
        last_usage_at: None,
    })
}

/// Build an `AccountRecord` for an API key auth entry.
pub fn account_record_from_api_key(api_key: &str, label: &str) -> AccountRecord {
    // Use a hash prefix of the key as the account_key to avoid storing the full key.
    let key_suffix = if api_key.len() > 8 {
        &api_key[api_key.len() - 8..]
    } else {
        api_key
    };
    AccountRecord {
        account_key: format!("apikey::{key_suffix}"),
        chatgpt_user_id: None,
        chatgpt_account_id: None,
        email: None,
        alias: label.to_string(),
        plan: None,
        auth_mode: Some("api_key".to_string()),
        created_at: Utc::now().timestamp(),
        last_used_at: None,
        last_usage: None,
        last_usage_at: None,
    }
}

// ─── CRUD Operations ───

/// Add or update an account in the registry. Returns `true` if it was a new account.
pub fn upsert_account(registry: &mut Registry, record: AccountRecord) -> bool {
    if let Some(existing) = registry
        .accounts
        .iter_mut()
        .find(|a| a.account_key == record.account_key)
    {
        // Merge: update fields that might have changed, keep alias and usage.
        existing.email = record.email.or_else(|| existing.email.clone());
        existing.plan = record.plan.or_else(|| existing.plan.clone());
        existing.auth_mode = record.auth_mode.or_else(|| existing.auth_mode.clone());
        existing.chatgpt_user_id = record
            .chatgpt_user_id
            .or_else(|| existing.chatgpt_user_id.clone());
        existing.chatgpt_account_id = record
            .chatgpt_account_id
            .or_else(|| existing.chatgpt_account_id.clone());
        false
    } else {
        registry.accounts.push(record);
        true
    }
}

/// Set the active account and record the activation timestamp.
pub fn set_active_account(registry: &mut Registry, account_key: &str) {
    registry.active_account_key = Some(account_key.to_string());
    registry.active_account_activated_at_ms = Some(Utc::now().timestamp_millis());
    // Update last_used_at on the activated account.
    if let Some(acct) = registry
        .accounts
        .iter_mut()
        .find(|a| a.account_key == account_key)
    {
        acct.last_used_at = Some(Utc::now().timestamp());
    }
}

/// Remove accounts by their keys. Returns the number removed.
pub fn remove_accounts(
    codex_home: &Path,
    registry: &mut Registry,
    keys_to_remove: &[String],
) -> usize {
    let mut removed = 0;
    for key in keys_to_remove {
        // Delete the per-account auth file.
        let auth_file = account_auth_path(codex_home, key);
        if let Err(e) = fs::remove_file(&auth_file)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!("Failed to delete {}: {e}", auth_file.display());
        }
        // Remove from registry.
        let before = registry.accounts.len();
        registry.accounts.retain(|a| a.account_key != *key);
        if registry.accounts.len() < before {
            removed += 1;
        }
        // Clear active if we just removed the active account.
        if registry.active_account_key.as_deref() == Some(key.as_str()) {
            registry.active_account_key = None;
            registry.active_account_activated_at_ms = None;
        }
    }
    removed
}

// ─── High-level command helpers ───

/// Copy the active `auth.json` to the accounts dir and register it.
/// This is the core of `codex pool login`: after `codex login` writes auth.json,
/// we capture it into the pool.
pub fn capture_active_auth(codex_home: &Path) -> std::io::Result<CaptureResult> {
    let auth_path = active_auth_path(codex_home);
    let contents = fs::read_to_string(&auth_path)?;
    let auth: AuthDotJson = serde_json::from_str(&contents)
        .map_err(|e| std::io::Error::other(format!("Failed to parse auth.json: {e}")))?;

    let record = if let Some(ref api_key) = auth.openai_api_key {
        // API key mode.
        account_record_from_api_key(api_key, "api-key")
    } else {
        account_record_from_auth(&auth).ok_or_else(|| {
            std::io::Error::other(
                "auth.json lacks token data (no id_token with user_id and account_id)",
            )
        })?
    };

    let account_key = record.account_key.clone();
    let label = record.display_label().to_string();

    // Copy auth file to accounts directory.
    ensure_accounts_dir(codex_home)?;
    let dest = account_auth_path(codex_home, &account_key);
    secure_copy_file(&auth_path, &dest)?;

    // Register in registry.
    let mut registry = load_registry(codex_home);
    let is_new = upsert_account(&mut registry, record);
    set_active_account(&mut registry, &account_key);
    save_registry(codex_home, &registry)?;

    info!(
        "Captured auth for '{}' ({})",
        label,
        if is_new { "new" } else { "updated" }
    );

    Ok(CaptureResult {
        account_key,
        label,
        is_new,
    })
}

pub struct CaptureResult {
    pub account_key: String,
    pub label: String,
    pub is_new: bool,
}

/// Import an auth.json file (or all .json files in a directory) into the pool.
pub fn import_path(codex_home: &Path, path: &Path) -> std::io::Result<Vec<ImportResult>> {
    if path.is_dir() {
        import_directory(codex_home, path)
    } else {
        let result = import_single_file(codex_home, path)?;
        Ok(vec![result])
    }
}

fn import_single_file(codex_home: &Path, path: &Path) -> std::io::Result<ImportResult> {
    let contents = fs::read_to_string(path)?;
    let auth: AuthDotJson = serde_json::from_str(&contents)
        .map_err(|e| std::io::Error::other(format!("Failed to parse {}: {e}", path.display())))?;

    let record = if let Some(ref api_key) = auth.openai_api_key {
        account_record_from_api_key(api_key, &path.display().to_string())
    } else {
        account_record_from_auth(&auth).ok_or_else(|| {
            std::io::Error::other(format!(
                "{}: lacks token data for account registration",
                path.display()
            ))
        })?
    };

    let account_key = record.account_key.clone();
    let label = record.display_label().to_string();

    ensure_accounts_dir(codex_home)?;
    let dest = account_auth_path(codex_home, &account_key);
    secure_copy_file(path, &dest)?;

    let mut registry = load_registry(codex_home);
    let is_new = upsert_account(&mut registry, record);
    save_registry(codex_home, &registry)?;

    Ok(ImportResult {
        label,
        is_new,
        error: None,
    })
}

fn import_directory(codex_home: &Path, dir: &Path) -> std::io::Result<Vec<ImportResult>> {
    let mut results = Vec::new();
    let mut entries: Vec<_> = fs::read_dir(dir)?
        .filter_map(std::result::Result::ok)
        .filter(|e| {
            e.path()
                .extension()
                .is_some_and(|ext| ext == "json")
        })
        .collect();
    entries.sort_by_key(std::fs::DirEntry::file_name);

    for entry in entries {
        let path = entry.path();
        match import_single_file(codex_home, &path) {
            Ok(r) => results.push(r),
            Err(e) => results.push(ImportResult {
                label: path.display().to_string(),
                is_new: false,
                error: Some(format!("{e}")),
            }),
        }
    }
    Ok(results)
}

pub struct ImportResult {
    pub label: String,
    pub is_new: bool,
    pub error: Option<String>,
}

/// Activate an account by key: copy its auth file to ~/.codex/auth.json.
pub fn activate_account(codex_home: &Path, account_key: &str) -> std::io::Result<()> {
    let source = account_auth_path(codex_home, account_key);
    if !source.exists() {
        return Err(std::io::Error::other(format!(
            "Auth file not found for account '{account_key}'"
        )));
    }

    let dest = active_auth_path(codex_home);
    secure_copy_file(&source, &dest)?;

    let mut registry = load_registry(codex_home);
    set_active_account(&mut registry, account_key);
    save_registry(codex_home, &registry)?;

    Ok(())
}

/// Find accounts matching a query string (case-insensitive substring on email/alias).
pub fn find_matching_accounts<'a>(
    accounts: &'a [AccountRecord],
    query: &str,
) -> Vec<&'a AccountRecord> {
    let q = query.to_lowercase();
    accounts
        .iter()
        .filter(|a| {
            a.email
                .as_deref()
                .is_some_and(|e| e.to_lowercase().contains(&q))
                || a.alias.to_lowercase().contains(&q)
                || a.account_key.to_lowercase().contains(&q)
        })
        .collect()
}

// ─── File helpers ───

/// Copy a file with restrictive permissions (0o600 on Unix).
fn secure_copy_file(src: &Path, dest: &Path) -> std::io::Result<()> {
    let contents = fs::read(src)?;
    let mut options = fs::OpenOptions::new();
    options.truncate(true).write(true).create(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    let mut file = options.open(dest)?;
    file.write_all(&contents)?;
    file.flush()?;
    Ok(())
}
