# FileSystemQueueBundle

**Filesystem-based queue transport for Mautic 7**  
Replaces or supplements default queues (email, hit, failed) with pure disk-based storage — similar to old Mautic 4 `var/spool` but using modern Symfony Messenger.

Perfect for high-volume installations (e.g. 500k+ emails/day) on servers with fast SSDs but limited RAM (e.g. 16 GB), where Redis or Beanstalkd consume too much memory.

## Features

- Pure disk storage — no RAM usage for queued messages (only active processing uses memory)
- Atomic file operations (rename-based locking) — safe for multiple parallel workers
- Separate directories for `email`, `hit`, `failed` (optional for sms/push if extended)
- Use native PHP serialize (no extra dependencies)
- No external services required (no Redis, Beanstalkd, RabbitMQ)
- Command to send email old way (without using `messanger:consume`)

## Requirements

- Mautic **7.x** (tested on 7.1.0 RC and PHP 8.5.3)
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
Configuration
Recommended: Set via configuration files (bypasses UI limitations)
Update app/config/local.php variables:
```PHP
'messenger_dsn_email'  => 'filesystem://default/email',
'messenger_dsn_hit'    => 'filesystem://default/hit',
'messenger_dsn_failed' => 'filesystem://default/failed',
```
Clear cache again.
This method is most reliable — the UI may not show filesystem in the dropdown, but the transport will be used.
Alternative: Configure via UI (if scheme appears after tagging)

1. Go to Configuration → Queue Settings
2. For each queue (Email, Hit, Failed):
* Scheme: filesystem
* Host: default
* Path: email (or hit, failed, etc.)
3. Save

If filesystem does not appear in dropdown → use config override above.

## Usage
### Run queue workers
Add to cron (recommended: separate processes for each queue type)
```Bash
# Email queue - high volume
* * * * * php /path/to/mautic/bin/console mautic:queue:consume email --no-interaction --limit=100 --time-limit=300 --memory-limit=512M

# Hit queue - lighter load
* * * * * php /path/to/mautic/bin/console mautic:queue:consume hit --no-interaction --limit=200 --time-limit=120

# Failed queue - occasional retries
* * * * * php /path/to/mautic/bin/console mautic:queue:consume failed --no-interaction --limit=50 --time-limit=300
```
Use **supervisor** or **systemd** for production (better than cron).
## Useful command flags
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
