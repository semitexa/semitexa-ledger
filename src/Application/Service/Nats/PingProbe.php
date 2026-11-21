<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Nats;

use Basis\Nats\Client;
use Basis\Nats\Message\Pong;

/**
 * Whether a connection answers a PING, for {@see NatsClient}'s liveness check.
 *
 * The library's ping() reads ONE frame, for at most its read timeout (1 s): a
 * frame ahead of the PONG (the pull's own 408 sent as the client stopped
 * reading, a late reply, an INFO) or a PONG a busy broker sent later made a
 * healthy connection look dead (release preflight, 2026-11-03). So the PONG
 * gets GRACE more to arrive, anything else read meanwhile being dropped (its
 * subscription is gone). No second PING: its PONG would stay on the socket
 * and cut the next pull short.
 */
final class PingProbe
{
    /**
     * Seconds a PONG may take past the library's read timeout (1 s): a
     * connection that never answers is declared dead 3 s after its PING.
     */
    private const GRACE = 2.0;

    public static function answered(Client $client): bool
    {
        if ($client->ping()) {
            return true;
        }

        $deadline = microtime(true) + self::GRACE;
        while (($left = $deadline - microtime(true)) > 0) {
            if ($client->connection?->getMessage($left) instanceof Pong) {
                return true;
            }
        }

        return false;
    }
}
