<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

/**
 * Column values as they travel in a replication event: JSON-safe, and decoded
 * back to exactly the bytes the database returned.
 *
 * A value that is not valid UTF-8 — a BINARY(16) UUID key, a blob — cannot be
 * a JSON string, so it travels as {"$b64": "..."}. Row keys become one string
 * the clock table can index: raw when printable, "b64:..." otherwise.
 */
final class RowCodec
{
    private const BINARY = '$b64';
    private const KEY_PREFIX = 'b64:';

    public static function encodeValue(mixed $value): mixed
    {
        if (is_string($value) && !mb_check_encoding($value, 'UTF-8')) {
            return [self::BINARY => base64_encode($value)];
        }

        return $value;
    }

    public static function decodeValue(mixed $value): mixed
    {
        if (is_array($value) && array_keys($value) === [self::BINARY] && is_string($value[self::BINARY])) {
            $decoded = base64_decode($value[self::BINARY], true);
            if ($decoded === false) {
                throw new \InvalidArgumentException('Malformed binary value in a replication event.');
            }

            return $decoded;
        }

        return $value;
    }

    public static function encodeKey(string $key): string
    {
        return mb_check_encoding($key, 'UTF-8') && !str_starts_with($key, self::KEY_PREFIX)
            ? $key
            : self::KEY_PREFIX . base64_encode($key);
    }

    public static function decodeKey(string $key): string
    {
        if (!str_starts_with($key, self::KEY_PREFIX)) {
            return $key;
        }

        $decoded = base64_decode(substr($key, strlen(self::KEY_PREFIX)), true);
        if ($decoded === false) {
            throw new \InvalidArgumentException('Malformed binary key in a replication event.');
        }

        return $decoded;
    }

    /** Whether two values read from the database are the same stored value. */
    public static function same(mixed $a, mixed $b): bool
    {
        if ($a === null || $b === null) {
            return $a === $b;
        }

        // Drivers return the same column as int or string depending on mode;
        // compare the stored representation, not the PHP type.
        return self::stored($a) === self::stored($b);
    }

    private static function stored(mixed $value): ?string
    {
        return match (true) {
            is_bool($value)                   => $value ? '1' : '0',
            is_int($value), is_float($value)  => (string) $value,
            is_string($value)                 => $value,
            default                           => null, // not a column value: never equal to one
        };
    }
}
