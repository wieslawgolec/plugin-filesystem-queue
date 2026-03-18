<?php
// File: plugins/FileSystemQeueueBundle/Config/config.php

return [
    'name'        => 'File System Queue Bundle',
    'description' => 'Provides filesystem transport for queues + commands to consume it in legacy Mautic 4 way',
    'version'     => '1.0.0',
    'author'      => 'Wieslaw Golec',
    'parameters' => [
        'filesystem_queue_batch_size' => 1,
        'filesystem_queue_batch_auto_shuffle' => true,
        'filesystem_queue_msg_limit' => null,
        'filesystem_queue_time_limit' => null,
        'filesystem_queue_email_msg_limit' => null,
        'filesystem_queue_email_time_limit' => null,
        'filesystem_queue_hit_msg_limit' => null,
        'filesystem_queue_hit_time_limit' => null,
        'filesystem_queue_failed_msg_limit' => null,
        'filesystem_queue_failed_time_limit' => null,
    ],
];
