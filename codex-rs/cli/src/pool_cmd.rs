//! `codex pool` subcommand — manage account pool for automatic quota switching.
//!
//! Commands mirror codex-auth's CLI but are built directly into Codex:
//!   codex pool login          — run OAuth login and add account to pool
//!   codex pool list           — show all pooled accounts with usage
//!   codex pool import <path>  — import auth.json file(s)
//!   codex pool switch [query] — switch active account
//!   codex pool remove [query] — remove account(s) from pool

use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use codex_core::config::find_codex_home;
use codex_login::auth::pool_registry;
use codex_login::auth::pool_registry::AccountRecord;
use codex_utils_cli::CliConfigOverrides;

/// Manage account pool for automatic quota switching.
#[derive(Debug, clap::Parser)]
pub struct PoolCli {
    #[clap(flatten)]
    pub config_overrides: CliConfigOverrides,

    #[command(subcommand)]
    pub subcommand: PoolSubcommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum PoolSubcommand {
    /// Log in via browser OAuth and add the account to the pool.
    Login,

    /// List all accounts in the pool with usage info.
    List {
        /// Output as JSON.
        #[arg(long)]
        json: bool,
    },

    /// Import auth.json file(s) into the pool.
    Import {
        /// Path to an auth.json file or a directory of .json files.
        path: PathBuf,
    },

    /// Switch the active account.
    Switch {
        /// Email, alias, or account key substring to match.
        /// If omitted, lists all accounts for selection.
        query: Option<String>,
    },

    /// Remove account(s) from the pool.
    Remove {
        /// Email, alias, or account key substring to match.
        query: Option<String>,

        /// Remove all accounts.
        #[arg(long)]
        all: bool,
    },
}

impl PoolCli {
    pub async fn run(self) -> Result<()> {
        let PoolCli {
            config_overrides: _,
            subcommand,
        } = self;

        let codex_home = find_codex_home().context("failed to resolve CODEX_HOME")?;

        match subcommand {
            PoolSubcommand::Login => run_pool_login(&codex_home).await?,
            PoolSubcommand::List { json } => run_pool_list(&codex_home, json)?,
            PoolSubcommand::Import { path } => run_pool_import(&codex_home, &path)?,
            PoolSubcommand::Switch { query } => run_pool_switch(&codex_home, query.as_deref())?,
            PoolSubcommand::Remove { query, all } => {
                run_pool_remove(&codex_home, query.as_deref(), all)?
            }
        }

        Ok(())
    }
}

// ─── Login ───

async fn run_pool_login(codex_home: &std::path::Path) -> Result<()> {
    eprintln!("Starting browser login flow...");
    eprintln!("After login, the account will be automatically added to the pool.");

    // Run the standard codex login flow which writes ~/.codex/auth.json.
    codex_cli::login::login_with_chatgpt(
        codex_home.to_path_buf(),
        None, // forced_chatgpt_workspace_id
        codex_login::AuthCredentialsStoreMode::File,
    )
    .await
    .context("Login failed")?;

    // Capture the newly written auth.json into the pool.
    let result =
        pool_registry::capture_active_auth(codex_home).context("Failed to register account")?;

    if result.is_new {
        eprintln!("✓ New account '{}' added to pool.", result.label);
    } else {
        eprintln!("✓ Account '{}' updated in pool.", result.label);
    }

    Ok(())
}

// ─── List ───

fn run_pool_list(codex_home: &std::path::Path, json: bool) -> Result<()> {
    // Clear expired exhaustions and get the up-to-date registry in one pass.
    let registry = pool_registry::clear_expired_exhaustions(codex_home);

    if registry.accounts.is_empty() {
        eprintln!("No accounts in pool. Run `codex pool login` to add one.");
        return Ok(());
    }

    if json {
        let out = serde_json::to_string_pretty(&registry.accounts)?;
        println!("{out}");
        return Ok(());
    }

    // Table header.
    println!(
        "{:<30} {:<10} {:>6} {:>8}  STATUS",
        "ACCOUNT", "PLAN", "5H%", "WEEKLY%"
    );
    println!("{}", "─".repeat(75));

    let now_ts = chrono::Utc::now().timestamp();
    for acct in &registry.accounts {
        let label = acct.display_label();
        let plan = acct.plan.as_deref().unwrap_or("-");
        let (primary_pct, weekly_pct) = format_usage(&acct.last_usage);

        let mut status_parts = Vec::new();
        if registry.active_account_key.as_deref() == Some(&acct.account_key) {
            status_parts.push("← active".to_string());
        }
        if let Some(until) = acct.exhausted_until
            && until > now_ts
        {
            let remaining_min = (until - now_ts + 59) / 60;
            status_parts.push(format!("exhausted ({remaining_min}m)"));
        }
        let status = status_parts.join(" ");

        println!(
            "{:<30} {:<10} {:>6} {:>8}  {}",
            truncate(label, 30),
            plan,
            primary_pct,
            weekly_pct,
            status
        );
    }

    eprintln!("\n{} account(s) in pool.", registry.accounts.len());
    Ok(())
}

// ─── Import ───

fn run_pool_import(codex_home: &std::path::Path, path: &std::path::Path) -> Result<()> {
    if !path.exists() {
        bail!("Path does not exist: {}", path.display());
    }

    let results = pool_registry::import_path(codex_home, path)?;

    let mut imported = 0;
    let mut updated = 0;
    let mut failed = 0;

    for r in &results {
        if let Some(ref err) = r.error {
            eprintln!("✗ {}: {err}", r.label);
            failed += 1;
        } else if r.is_new {
            eprintln!("✓ Imported: {}", r.label);
            imported += 1;
        } else {
            eprintln!("✓ Updated: {}", r.label);
            updated += 1;
        }
    }

    eprintln!(
        "\nDone: {imported} imported, {updated} updated, {failed} failed."
    );
    Ok(())
}

// ─── Switch ───

fn run_pool_switch(codex_home: &std::path::Path, query: Option<&str>) -> Result<()> {
    // Clear expired exhaustions and get the up-to-date registry in one pass.
    let registry = pool_registry::clear_expired_exhaustions(codex_home);

    if registry.accounts.is_empty() {
        bail!("No accounts in pool.");
    }

    let target = match query {
        Some(q) => {
            let matches = pool_registry::find_matching_accounts(&registry.accounts, q);
            match matches.len() {
                0 => bail!("No account matches '{q}'."),
                1 => matches[0].clone(),
                _ => {
                    eprintln!("Multiple accounts match '{q}':");
                    print_numbered_list(&matches);
                    let idx = read_selection(matches.len())?;
                    matches[idx].clone()
                }
            }
        }
        None => {
            // Interactive selection: show numbered list.
            let all_refs: Vec<&AccountRecord> = registry.accounts.iter().collect();
            eprintln!("Select account to activate:");
            print_numbered_list(&all_refs);
            let idx = read_selection(all_refs.len())?;
            registry.accounts[idx].clone()
        }
    };

    pool_registry::activate_account(codex_home, &target.account_key)
        .context("Failed to activate account")?;

    eprintln!("✓ Switched to '{}'.", target.display_label());
    Ok(())
}

// ─── Remove ───

fn run_pool_remove(
    codex_home: &std::path::Path,
    query: Option<&str>,
    all: bool,
) -> Result<()> {
    let mut registry = pool_registry::load_registry(codex_home);

    if registry.accounts.is_empty() {
        bail!("No accounts in pool.");
    }

    let keys_to_remove: Vec<String> = if all {
        registry
            .accounts
            .iter()
            .map(|a| a.account_key.clone())
            .collect()
    } else if let Some(q) = query {
        let matches = pool_registry::find_matching_accounts(&registry.accounts, q);
        match matches.len() {
            0 => bail!("No account matches '{q}'."),
            _ => matches.iter().map(|a| a.account_key.clone()).collect(),
        }
    } else {
        let all_refs: Vec<&AccountRecord> = registry.accounts.iter().collect();
        eprintln!("Select account to remove:");
        print_numbered_list(&all_refs);
        let idx = read_selection(all_refs.len())?;
        vec![registry.accounts[idx].account_key.clone()]
    };

    let removed = pool_registry::remove_accounts(codex_home, &mut registry, &keys_to_remove);
    pool_registry::save_registry(codex_home, &registry)?;

    eprintln!("✓ Removed {removed} account(s).");

    // If we removed the active account, try to activate the next best.
    if registry.active_account_key.is_none() && !registry.accounts.is_empty() {
        let next = &registry.accounts[0];
        pool_registry::activate_account(codex_home, &next.account_key)?;
        eprintln!(
            "  Activated '{}' as the new active account.",
            next.display_label()
        );
    }

    Ok(())
}

// ─── Helpers ───

fn format_usage(usage: &Option<pool_registry::LastUsage>) -> (String, String) {
    let Some(u) = usage else {
        return ("-".into(), "-".into());
    };
    let primary = u
        .primary
        .as_ref()
        .and_then(|w| w.used_percent)
        .map(|p| format!("{p:.0}%"))
        .unwrap_or_else(|| "-".into());
    let weekly = u
        .secondary
        .as_ref()
        .and_then(|w| w.used_percent)
        .map(|p| format!("{p:.0}%"))
        .unwrap_or_else(|| "-".into());
    (primary, weekly)
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let end = s
            .char_indices()
            .nth(max - 1)
            .map(|(i, _)| i)
            .unwrap_or(s.len());
        format!("{}…", &s[..end])
    }
}

fn print_numbered_list(accounts: &[&AccountRecord]) {
    for (i, acct) in accounts.iter().enumerate() {
        let label = acct.display_label();
        let plan = acct.plan.as_deref().unwrap_or("-");
        eprintln!("  [{}] {} ({})", i + 1, label, plan);
    }
}

fn read_selection(count: usize) -> Result<usize> {
    use std::io::BufRead;
    use std::io::Write;
    eprint!("Enter number (1-{count}): ");
    std::io::stderr().flush().ok();
    let mut line = String::new();
    std::io::stdin()
        .lock()
        .read_line(&mut line)
        .context("Failed to read input")?;
    let n: usize = line.trim().parse().context("Invalid number")?;
    if n < 1 || n > count {
        bail!("Selection out of range.");
    }
    Ok(n - 1)
}
