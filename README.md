# FileSystemQueueBundle

**Filesystem-based queue transport for Mautic 7**  
Replaces or supplements default queues (email, hit, failed) with pure disk-based storage — similar to old Mautic 4 `var/spool` but using modern Symfony Messenger.

Perfect for high-volume installations (e.g. 500k+ emails/day) on servers with fast SSDs but limited RAM (e.g. 16 GB), where Redis or Beanstalkd consume too much memory.

**Current version: 1.1.0** (hardened transport with Mautic-4 style retry state machine)

## Features

- Pure disk storage — no RAM usage for queued messages (only active processing uses memory)
- **Atomic rename-based claiming** — safe for multiple parallel workers (no double-processing)
- **Mautic-4 style retry state machine** — temporary failures stay in the same queue; final failures go to the failed transport
- **FilesystemRetryMiddleware** — owns temporary retries so the worker always acks (no duplication into failed)
- Exponential backoff on requeue (configurable base delay)
- Stuck `.processing` recovery (timeout-based reclaim)
- Keepalive support (mtime refresh without `touch()`)
- Separate directories for `email`, `hit`, `failed` (extensible to sms/push if needed)
- Native PHP serialization (no extra dependencies)
- No external services required (no Redis, Beanstalkd, RabbitMQ)
- Legacy-style send commands still available for bypassing Messenger if needed
- Built-in **supervisor** command for dynamic, auto-scaling email workers (`mautic:emails:supervisor`)
- PHPUnit suite + concurrent multi-process race tests + GitHub Actions (PHP 8.2 / 8.5 / 8.6)

## Requirements

- Mautic **7.x** (tested on 7.1.0+ and PHP 8.x)
- PHP 8.1+
- Fast SSD recommended (high IOPS for many small files)

## Installation

```text
1. Copy the plugin folder into `plugins/` as `FileSystemQueueBundle`:

plugins/
   └── FileSystemQueueBundle/

2. Log in to Mautic admin → Settings → Plugins
   → Click Install/Upgrade Plugins
   → The bundle should appear → install it

3. Clear cache (important after plugin install):
```

```bash
php bin/console cache:clear
```

## Configuration

**Recommended: override DSNs in** `app/config/local.php` (most reliable):

```php
'messenger_dsn_email'  => 'filesystem://default/email',
'messenger_dsn_hit'    => 'filesystem://default/hit',
'messenger_dsn_failed' => 'filesystem://default/failed',
```

Clear cache after changes:

```bash
php bin/console cache:clear
```

**Alternative (UI method, if filesystem appears in the dropdown):**

1. Go to **Configuration** → **Queue Settings**
2. For each queue (Email, Hit, Failed):
   - Scheme: `filesystem`
   - Host: `default`
   - Path: `email` (or `hit`, `failed`, etc.)
3. Save

If filesystem does not appear in the dropdown → use the config override above.

Queue files are stored under:

```text
{projectDir}/var/queue/email/
{projectDir}/var/queue/hit/
{projectDir}/var/queue/failed/
```

## Retry state machine (v1.1)

Messages move through well-defined filename states. Rename is the only locking primitive.

| Stage | Filename | Who acts |
|-------|----------|----------|
| First enqueue | `{id}.message` | `send()` |
| Claim | `{id}.processing` | `tryClaim()` |
| Temporary fail | rename → `{id}.message.retry.N` | Middleware + `requeueWithRetry()` → Worker `ack()` |
| Claim after retry | `{id}.processing.retry.N` | `tryClaim()` |
| Final fail | file deleted | Worker `reject()` → Messenger failed transport |
| Stuck recovery | `.processing*` → `.message` / `.message.retry.N` | `recoverStuckProcessingFiles()` |

**Guarantees:**

- A message is never present in both the active queue and the failed queue at the same time.
- Temporary failures do **not** create a second copy; the same file is renamed back to ready.
- Concurrent workers cannot claim the same message (atomic `rename()`; loser gets `null`).

Backoff is applied via future `mtime` on the ready file after a temporary failure (see `filesystem_queue_retry_backoff_base`).

## Plugin settings

All settings are overridable in `app/config/local.php` (e.g. `'filesystem_queue_batch_size' => 50`).

### Batching & limits

| Parameter | Default | Description |
|-----------|---------|-------------|
| `filesystem_queue_batch_size` | `1` | Messages claimed per `get()` call |
| `filesystem_queue_batch_auto_shuffle` | `true` | Shuffle ready IDs before claiming |
| `filesystem_queue_msg_limit` | `null` | Global max messages per consume run |
| `filesystem_queue_time_limit` | `null` | Global time limit (seconds) per consume run |
| `filesystem_queue_email_msg_limit` | `null` | Email-queue specific message limit |
| `filesystem_queue_email_time_limit` | `null` | Email-queue specific time limit |
| `filesystem_queue_hit_msg_limit` | `null` | Hit-queue specific message limit |
| `filesystem_queue_hit_time_limit` | `null` | Hit-queue specific time limit |
| `filesystem_queue_failed_msg_limit` | `null` | Failed-queue specific message limit |
| `filesystem_queue_failed_time_limit` | `null` | Failed-queue specific time limit |

### Retry & recovery (v1.1)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `filesystem_queue_max_attempts` | `5` | Max attempts before final failure (reject → failed transport) |
| `filesystem_queue_recovery_timeout` | `300` | Seconds before a stuck `.processing` file is reclaimed |
| `filesystem_queue_recovery_interval` | `30` | How often recovery runs (seconds) |
| `filesystem_queue_recovery_stuck` | `true` | Enable stuck-file recovery |
| `filesystem_queue_retry_backoff_base` | `30` | Base delay (seconds) for exponential backoff (`base * 2^(attempt-1)`, capped at 3600) |
| `filesystem_queue_send_max_retries` | `15` | Max retries when writing a new message file (collision / FS errors) |

### Supervisor (`mautic:emails:supervisor`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `filesystem_queue_supervisor_initial_threads` | `1` | Starting number of worker processes |
| `filesystem_queue_supervisor_max_threads` | `8` | Maximum concurrent workers |
| `filesystem_queue_supervisor_time_limit` | `3600` | Max lifetime (seconds) per thread |
| `filesystem_queue_supervisor_memory_limit` | `'256M'` | Memory limit per thread |
| `filesystem_queue_supervisor_email_limit` | `300` | Target emails per thread per cycle |
| `filesystem_queue_supervisor_settle_time` | `15` | Seconds to wait after start/stop |
| `filesystem_queue_supervisor_emails_per_extra_thread` | `250` | Expected extra emails per added thread |
| `filesystem_queue_supervisor_emails_per_second_initial` | `0.8` | Initial assumed send rate |
| `filesystem_queue_supervisor_rate_smoothing_factor` | `0.3` | Rate smoothing factor (0–1) |
| `filesystem_queue_supervisor_dynamic_rate_enabled` | `true` | Scale based on observed rate |
| `filesystem_queue_supervisor_max_messages_per_thread` | `0` | Hard cap per thread (`0` = unlimited) |
| `filesystem_queue_supervisor_max_server_load` | `4.0` | Max load average before refusing new threads |
| `filesystem_queue_supervisor_max_memory_usage_percent` | `80` | Max system memory % before scaling down |
| `filesystem_queue_supervisor_delay_between_threads` | `5` | Delay (seconds) between starting threads |
| `filesystem_queue_supervisor_max_load_increase` | `0.5` | Max load increase per scaling step |
| `filesystem_queue_supervisor_max_mem_increase_percent` | `5` | Max memory % increase per step |
| `filesystem_queue_supervisor_check_interval` | `10` | How often supervisor re-evaluates |
| `filesystem_queue_supervisor_logging_enabled` | `true` | Log supervisor decisions |
| `filesystem_queue_supervisor_custom_log_name` | `''` | Custom log name (empty = default in `var/logs/`) |
| `filesystem_queue_supervisor_benchmark_enabled` | `false` | Enable benchmark metrics in threads |
| `filesystem_queue_supervisor_verbosity_level` | `0` | Verbosity: 0 / 1 / 2 / 3 |
| `filesystem_queue_supervisor_check_maintenance_locks` | `false` | Respect Mautic maintenance locks |

## Usage

### Basic queue workers (cron or systemd)

```bash
# Email queue – high volume
* * * * * php /path/to/mautic/bin/console mautic:queue:consume email --no-interaction --limit=100 --time-limit=300 --memory-limit=512M

# Hit queue – lighter load
* * * * * php /path/to/mautic/bin/console mautic:queue:consume hit --no-interaction --limit=200 --time-limit=120

# Failed queue – occasional retries
* * * * * php /path/to/mautic/bin/console mautic:queue:consume failed --no-interaction --limit=50 --time-limit=300
```

### Built-in supervisor for emails

Prefer **systemd** / **supervisor** over plain cron for production:

```bash
# Foreground (use nohup/screen/systemd in production)
php bin/console mautic:emails:supervisor

# Verbose + benchmark
php bin/console mautic:emails:supervisor -vv --benchmark
```

The supervisor starts with `initial_threads`, monitors load/memory, and scales up to `max_threads` based on send rate and thresholds. Threads are restarted when they hit time/memory limits.

### Useful flags

- `--benchmark` — messages/sec, total time, peak memory
- `-v` / `-vv` / `-vvv` — increasing verbosity
- `--memory-limit=128M` — exit if memory exceeds limit
- `--limit=500` / `--time-limit=300` — batch/time controls

```bash
php bin/console mautic:emails:send -v
php bin/console mautic:emails:send --benchmark --limit=500
php bin/console mautic:hits:consume --benchmark
php bin/console mautic:failed:consume --memory-limit=128M
```

## Monitoring

```bash
# Ready messages (including retries)
ls var/queue/email/*.message* 2>/dev/null | wc -l

# Currently processing
ls var/queue/email/*.processing* 2>/dev/null | wc -l

# Retry backlog only
ls var/queue/email/*.message.retry.* 2>/dev/null | wc -l
```

Integrate directory sizes with Prometheus, Zabbix, or similar as needed.

## Testing

The plugin ships with a PHPUnit suite (unit + concurrent multi-process race tests).

### Run locally

```bash
cd plugins/FileSystemQueueBundle
composer install
vendor/bin/phpunit
```

### What is covered

- **Unit:** `send` / `tryClaim` / `ack` / `reject` / `requeueWithRetry` / recovery / keepalive / factory / middleware decisions
- **Concurrency:** parallel workers claiming the same pool (exactly-once), concurrent `send()` uniqueness, requeue + reclaim under contention, interleaved send+claim accounting

### CI

GitHub Actions runs the suite on **PHP 8.2, 8.5, and 8.6** on every push/PR to `mautic7.x` and `feature/**` branches (see `.github/workflows/tests.yml`).

> Note: CI installs without the private `mautic/core-lib` package and uses a minimal `CoreParametersHelper` stub. Production always uses the real Mautic class.

## Architecture notes (v1.1)

| Component | Role |
|-----------|------|
| `FileSystemTransport` | Producer + consumer; atomic claim; requeue; recovery; keepalive |
| `FileSystemTransportFactory` | Builds transport from `filesystem://…` DSN under `{projectDir}/var/queue` |
| `FilesystemRetryMiddleware` | On temporary failure: `requeueWithRetry()` and return (worker acks). On final failure: re-throw (worker rejects → failed transport) |
| `AttemptStamp` | Carries attempt number on the envelope (mirrors filename state) |

## Troubleshooting

- **"Unsupported scheme" in UI** → Use DSN override in `local.php`
- **No files appear** → Check `var/logs/`, workers running, DSN path, directory permissions on `var/queue/`
- **Factory not registered** → `php bin/console debug:container --tag messenger.transport_factory`
- **Messages stuck in `.processing`** → Recovery will reclaim after `filesystem_queue_recovery_timeout`; ensure `filesystem_queue_recovery_stuck` is `true`
- **Too many rapid retries** → Increase `filesystem_queue_retry_backoff_base` or lower worker concurrency temporarily

## License

MIT

## Author

Wieslaw Golec

Feel free to contribute or report issues.
