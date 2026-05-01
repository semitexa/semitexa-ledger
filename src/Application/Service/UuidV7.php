<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service;

/**
 * Generates UUID version 7 (time-ordered, random).
 *
 * Format: xxxxxxxx-xxxx-7xxx-yxxx-xxxxxxxxxxxx
 *
 *   48 bits  timestamp in milliseconds (sortable)
 *    4 bits  version = 7
 *   12 bits  random
 *    2 bits  variant = 10 (RFC 4122)
 *   62 bits  random
 *
 * Multiple calls within the same millisecond will still produce unique IDs
 * because the random bits differ.
 */
final class UuidV7
{
    public static function generate(): string
    {
        $tsMs = (int) (microtime(true) * 1000);
        $tsHex = str_pad(dechex($tsMs), 12, '0', STR_PAD_LEFT);

        // 8 hex chars: upper 32 bits of 48-bit ms timestamp
        $part1 = substr($tsHex, 0, 8);
        // 4 hex chars: lower 16 bits of 48-bit ms timestamp
        $part2 = substr($tsHex, 8, 4);
        // 4 hex chars: version nibble '7' + 12 random bits
        $part3 = sprintf('7%03x', random_int(0, 0xFFF));
        // 4 hex chars: variant bits '10' (0x8000–0xBFFF) + 14 random bits
        $part4 = sprintf('%04x', random_int(0x8000, 0xBFFF));
        // 12 hex chars: 48 random bits
        $part5 = sprintf('%012x', random_int(0, 0xFFFFFFFFFFFF));

        return "{$part1}-{$part2}-{$part3}-{$part4}-{$part5}";
    }
}
