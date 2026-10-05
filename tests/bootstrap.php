<?php

declare(strict_types=1);

// When running standalone (no full Mautic), mautic/core-lib may be missing.
// Tell Composer to ignore the platform requirement for that package so
// require-dev packages can still be installed.
if (!file_exists(dirname(__DIR__) . '/vendor/mautic/core-lib')) {
    // Stub is loaded via composer autoload-dev "files".
}

require dirname(__DIR__) . '/vendor/autoload.php';
