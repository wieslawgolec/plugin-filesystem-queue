<?php
// File: plugins/FileSystemQeueueBundle/Config/config.php

return [
    'name'        => 'File System Queue Bundle',
    'description' => 'Provides filesystem transport for queues + commands to consume it in legacy Mautic 4 way',
    'version'     => '1.0.0',
    'author'      => 'Wieslaw Golec',
    'parameters' => [
        'filesystem_queue_batch_size'         => 1,
        'filesystem_queue_recovery_interval'  => 30,
        'filesystem_queue_recovery_stuck'     => true,
        'filesystem_queue_recovery_timeout'   => 3600,
        'filesystem_queue_batch_auto_shuffle' => true,
        'filesystem_queue_msg_limit'          => null,
        'filesystem_queue_time_limit'         => null,
        'filesystem_queue_email_msg_limit'    => null,
        'filesystem_queue_email_time_limit'   => null,
        'filesystem_queue_hit_msg_limit'      => null,
        'filesystem_queue_hit_time_limit'     => null,
        'filesystem_queue_failed_msg_limit'   => null,
        'filesystem_queue_failed_time_limit'  => null,

        // Supervisor tuning parameters (used by mautic:emails:supervisor)
        'filesystem_queue_supervisor_initial_threads'              => 1,
        'filesystem_queue_supervisor_max_threads'                  => 8,
        'filesystem_queue_supervisor_time_limit'                   => 3600,         // seconds per thread
        'filesystem_queue_supervisor_memory_limit'                 => '256M',
        'filesystem_queue_supervisor_email_limit'                  => 300,
        'filesystem_queue_supervisor_settle_time'                  => 15,
        'filesystem_queue_supervisor_emails_per_extra_thread'      => 250,
        'filesystem_queue_supervisor_emails_per_second_initial'    => 0.8,          // average email sent per second
        'filesystem_queue_supervisor_rate_smoothing_factor'        => 0.3,
        'filesystem_queue_supervisor_dynamic_rate_enabled'         => true,
        'filesystem_queue_supervisor_max_messages_per_thread'      => 0,            // 0 = no cap (use mautic:emails:advanced-send command cap)
        'filesystem_queue_supervisor_max_server_load'              => 4.0,
        'filesystem_queue_supervisor_max_memory_usage_percent'     => 80,
        'filesystem_queue_supervisor_delay_between_threads'        => 5,            // seconds
        'filesystem_queue_supervisor_max_load_increase'            => 0.5,
        'filesystem_queue_supervisor_max_mem_increase_percent'     => 5,
        'filesystem_queue_supervisor_check_interval'               => 10,           // seconds
        'filesystem_queue_supervisor_logging_enabled'              => true,
        'filesystem_queue_supervisor_custom_log_name'              => '',           // leave empty for default: mautic_filesystem_queue_supervisor.log
        'filesystem_queue_supervisor_benchmark_enabled'            => false,
        'filesystem_queue_supervisor_verbosity_level'              => 0,            // 0 = none, 1 = -v, 2 = -vv, 3 = -vvv
        'filesystem_queue_supervisor_check_maintenance_locks'      => false,
    ],
];
