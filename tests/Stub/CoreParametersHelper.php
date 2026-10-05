<?php

declare(strict_types=1);

/**
 * Minimal stub of Mautic\CoreBundle\Helper\CoreParametersHelper
 * so unit tests can run without the full mautic/core-lib package.
 *
 * Only loaded via autoload-dev; production always uses the real class.
 */
namespace Mautic\CoreBundle\Helper;

if (!class_exists(CoreParametersHelper::class, false)) {
    class CoreParametersHelper
    {
        public function get(string $key, mixed $default = null): mixed
        {
            return $default;
        }
    }
}
