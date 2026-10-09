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

See the full parameter tables in the repository history / docs for batching, retry, recovery, and supervisor settings.

## Usage

### Basic queue workers (cron or systemd)

```bash
* * * * * php /path/to/mautic/bin/console mautic:queue:consume email --no-interaction --limit=100 --time-limit=300 --memory-limit=512M
* * * * * php /path/to/mautic/bin/console mautic:queue:consume hit --no-interaction --limit=200 --time-limit=120
* * * * * php /path/to/mautic/bin/console mautic:queue:consume failed --no-interaction --limit=50 --time-limit=300
```

### Built-in supervisor for emails

```bash
php bin/console mautic:emails:supervisor
php bin/console mautic:emails:supervisor -vv --benchmark
```

## Testing

```bash
cd plugins/FileSystemQueueBundle
composer install
vendor/bin/phpunit
```

CI runs on **PHP 8.2, 8.5, and 8.6**.

## Support the project

If this plugin saves you time, you can support development:

- **GitHub Sponsors:** [github.com/sponsors/wieslawgolec](https://github.com/sponsors/wieslawgolec)
- **Buy Me a Coffee:** [buymeacoffee.com/wieslawgolec](https://buymeacoffee.com/wieslawgolec)

## License

MIT

## Author

Wieslaw Golec

Feel free to contribute or report issues.
