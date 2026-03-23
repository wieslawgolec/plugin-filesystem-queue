# FileSystemQueueBundle

**Filesystem-based queue transport for Mautic 7**  
Replaces or supplements default queues (email, hit, failed) with pure disk-based storage — similar to old Mautic 4 `var/spool` but using modern Symfony Messenger.

Perfect for high-volume installations (e.g. 500k+ emails/day) on servers with fast SSDs but limited RAM (e.g. 16 GB), where Redis or Beanstalkd consume too much memory.

## Features

- Pure disk storage — no RAM usage for queued messages (only active processing uses memory)
- Atomic file operations (rename-based locking) — safe for multiple parallel workers
- Separate directories for `email`, `hit`, `failed` (extensible to sms/push if needed)
- Native PHP serialization (no extra dependencies)
- No external services required (no Redis, Beanstalkd, RabbitMQ)
- Legacy-style send commands (e.g. `mautic:emails:send`) still available for bypassing Messenger if needed
- Built-in **supervisor** command for dynamic, auto-scaling email sending workers (`mautic:emails:supervisor`)

## Requirements

- Mautic **7.x** (tested on 7.1.0 RC and PHP 8.x)
- PHP 8.1+
- Fast SSD recommended (high IOPS for many small files)

## Installation

```text
1. Copy the plugin folder into `plugins/` as `FileSystemQueueBundle`:

plugins/
   └── FileSystemQueueBundle/

2. Log in to Mautic admin → **Settings → Plugins**  
   → Click **Install/Upgrade Plugins**  
   → The bundle should appear → install it

3. Clear cache (important after plugin install):
```

```bash
php bin/console cache:clear
```
## Configuration
**Recommended: Override DSNs in** app/config/local.php (most reliable, bypasses UI issues):
```PHP
'messenger_dsn_email'  => 'filesystem://default/email',
'messenger_dsn_hit'    => 'filesystem://default/hit',
'messenger_dsn_failed' => 'filesystem://default/failed',
```
Clear cache again after changes:
```bash
php bin/console cache:clear
```
**Alternative (UI method, if filesystem appears in dropdown):**
1. Go to **Configuration** → **Queue Settings**
2. For each queue (Email, Hit, Failed):
* Scheme: filesystem
* Host: default
* Path: email (or hit, failed, etc.)
3. Save

If filesystem does not appear in dropdown → use config override above.

## Plugin Settings (from Config/config.php)
All settings below are overridable in app/config/local.php (e.g. 'filesystem_queue_batch_size' => 50,).
They control batching, limits, and especially the built-in supervisor behavior for email sending.
* filesystem_queue_batch_size (default: 1)
  → Number of messages processed in one batch inside the transport fetch logic.
* filesystem_queue_batch_auto_shuffle (default: true)
  → Whether to randomly shuffle messages in a batch (helps with fairness/distribution).
* filesystem_queue_msg_limit (default: null)
  → Global max messages to process per consume run (applies if no queue-specific limit set).
* filesystem_queue_time_limit (default: null)
  → Global time limit (seconds) per consume run.
* filesystem_queue_email_msg_limit (default: null)
  → Max messages per run specifically for the email queue.
* filesystem_queue_email_time_limit (default: null)
  → Time limit (seconds) per run for the email queue.
* filesystem_queue_hit_msg_limit (default: null)
  → Max messages per run for the hit queue.
* filesystem_queue_hit_time_limit (default: null)
  → Time limit (seconds) per run for the hit queue.
* filesystem_queue_failed_msg_limit (default: null)
  → Max messages per run for the failed queue.
* filesystem_queue_failed_time_limit (default: null)
  → Time limit (seconds) per run for the failed queue.

**Supervisor-specific settings** (used exclusively by the mautic:emails:supervisor command):

* filesystem_queue_supervisor_initial_threads (default: 1)
  → Starting number of parallel email-sending threads/processes.
* filesystem_queue_supervisor_max_threads (default: 8)
  → Maximum number of threads the supervisor can scale up to.
* filesystem_queue_supervisor_time_limit (default: 3600)
  → Max lifetime (seconds) for each thread before restart.
* filesystem_queue_supervisor_memory_limit (default: '256M')
  → Memory limit per thread (PHP - memory_limit equivalent).
* filesystem_queue_supervisor_email_limit (default: 300)
  → Target emails to send per thread per cycle (influences scaling).
* filesystem_queue_supervisor_settle_time (default: 15)
  → Seconds to wait after starting/stopping threads for stabilization.
* filesystem_queue_supervisor_emails_per_extra_thread (default: 250)
  → Additional emails expected per extra thread when scaling up.
* filesystem_queue_supervisor_emails_per_second_initial (default: 0.8)
  → Initial assumed send rate (emails/sec) used for early scaling decisions.
* filesystem_queue_supervisor_rate_smoothing_factor (default: 0.3)
  → How aggressively to smooth observed send rates (0–1).
* filesystem_queue_supervisor_dynamic_rate_enabled (default: true)
  → Whether to dynamically adjust scaling based on real send performance.
* filesystem_queue_supervisor_max_messages_per_thread (default: 0)
  → Hard cap on messages per thread (0 = unlimited, falls back to command caps).
* filesystem_queue_supervisor_max_server_load (default: 4.0)
  → Max allowed system load average before refusing to start new threads.
* filesystem_queue_supervisor_max_memory_usage_percent (default: 80)
  → Max system memory usage % before scaling down or refusing new threads.
* filesystem_queue_supervisor_delay_between_threads (default: 5)
  → Seconds delay when starting consecutive threads.
* filesystem_queue_supervisor_max_load_increase (default: 0.5)
  → Max allowed increase in load average per scaling step.
* filesystem_queue_supervisor_max_mem_increase_percent (default: 5)
  → Max allowed memory % increase per scaling step.
* filesystem_queue_supervisor_check_interval (default: 10)
  → How often (seconds) the supervisor checks system metrics and adjusts.
* filesystem_queue_supervisor_logging_enabled (default: true)
  → Whether to log supervisor actions/decisions.
* filesystem_queue_supervisor_custom_log_name (default: '')
  → Custom log filename (empty = default: mautic_filesystem_queue_supervisor.log in var/logs/).
* filesystem_queue_supervisor_benchmark_enabled (default: false)
  → Enable benchmark mode in supervisor threads (adds performance metrics).
* filesystem_queue_supervisor_verbosity_level (default: 0)
  → Verbosity for supervisor threads: 0=none, 1=-v, 2=-vv, 3=-vvv.
* filesystem_queue_supervisor_check_maintenance_locks (default: false)
  → Whether supervisor should respect Mautic maintenance/IP locks before scaling.
## Usage
### Basic Queue Workers (via cron or supervisor/systemd)
```Bash
# Email queue - high volume
* * * * * php /path/to/mautic/bin/console mautic:queue:consume email --no-interaction --limit=100 --time-limit=300 --memory-limit=512M

# Hit queue - lighter load
* * * * * php /path/to/mautic/bin/console mautic:queue:consume hit --no-interaction --limit=200 --time-limit=120

# Failed queue - occasional retries
* * * * * php /path/to/mautic/bin/console mautic:queue:consume failed --no-interaction --limit=50 --time-limit=300
```
### Using the Built-in Supervisor for Emails
Use **supervisor** or **systemd** for production (better than cron).
For production high-volume email sending, use the dynamic supervisor command instead of plain consume or cron:
```Bash
# Start supervisor (runs in foreground; use nohup/screen/systemd in production)
php bin/console mautic:emails:supervisor

# Example with verbosity and benchmark
php bin/console mautic:emails:supervisor -vv --benchmark
```
The supervisor starts with initial_threads, monitors system load/memory, and dynamically scales up to max_threads based on send rate and configured thresholds.
It restarts threads when they hit time/memory limits and logs decisions if enabled.

### Useful Command Flags (for consume/supervisor/benchmark)

* --benchmark — show messages/sec, total time, peak memory
* -v / -vv / -vvv — increasing verbosity
* --memory-limit=128M — exit if memory exceeds limit
  --limit=500 / --time-limit=300 — batch/time controls

```Bash
# Show basic progress & errors for email queue
php bin/console mautic:emails:send -v

# More detailed – shows each message being handled for hits queue
php bin/console mautic:emails:send -vv

# Very verbose – full debug + message content (careful in production!)
php bin/console mautic:emails:send -vvv

# Measure real performance (messages/sec, total time, memory peak, etc.)
php bin/console mautic:emails:send --benchmark --limit=500

# Consume hits messages & measure real performance (messages/sec, total time, memory peak, etc.)
php bin/console mautic:hits:consume --benchmark

# Consume falied messages & exit when done or memory use above 128M
php bin/console mautic:falied:consume --memory-limit=128M
```
The **--benchmark** flag is especially useful when tuning **--limit**, **--time-limit** or comparing different storage locations / SSDs.

## Monitoring

* Ready queue size:
```Bash
ls -l var/queue/email/*.message | wc -l
```
* Processing queue size:
```Bash
ls -l var/queue/email/*.processing | wc -l
```
* Current backlogs: watch the directories or integrate with monitoring (Prometheus, Zabbix, etc.)

## Troubleshooting

* "Unsupported scheme" in UI → Use config override in local.php (most reliable)
* No files appear → Check logs (var/logs/prod-*.log), ensure workers are running, verify DSN
* Factory not registered → Run php bin/console debug:container --tag messenger.transport_factory

# License
MIT
# Author
Wieslaw Golec

Feel free to contribute / report issues.
