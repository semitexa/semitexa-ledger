<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Nats;

use Basis\Nats\Client;
use Basis\Nats\Message\Ping;
use Basis\Nats\Message\Pong;

/**
 * Whether a connection answers a PING, for {@see NatsClient}'s liveness check.
 *
 * Not the library's ping(): that reads ONE frame, for at most its read timeout
 * (1 s). A frame ahead of the PONG (the pull's own 408 sent as the client
 * stopped reading, a late reply, an INFO) made it return false, and a PONG a
 * busy broker sent later made it return false or, once the connection's last
 * PONG is older than the library's pingInterval (2 s, so on every long-lived
 * connection), throw "Socket read timeout": a healthy connection was dropped
 * (release preflight, 2026-11-03). So this sends the PING itself and reads
 * until the PONG or the deadline. One PING only: a second one's PONG would
 * stay on the socket and cut the next pull short.
 *
 * MSG/HMSG frames read while waiting are NOT dispatched to their handlers,
 * they are dropped. Harmless today: the check runs after a pull has
 * unsubscribed its inbox, and pullers and CommandProcessor hold separate
 * connections. A connection that both pulls and holds a live subscription
 * would lose those messages here.
 */
final class PingProbe
{
    /** Seconds the PONG has to arrive: a connection that never answers is declared dead this long after its PING. */
    private const DEADLINE = 3.0;

    /** What Connection::getMessage() throws leaving on its timeout once the last PONG is stale. */
    private const READ_TIMEOUT = 'Socket read timeout';

    public static function answered(Client $client): bool
    {
        $connection = $client->connection;
        if ($connection === null) {
            return false;
        }

        $connection->sendMessage(new Ping());
        $deadline = microtime(true) + self::DEADLINE;
        while (($left = $deadline - microtime(true)) > 0) {
            try {
                if ($connection->getMessage($left) instanceof Pong) {
                    return true;
                }
            } catch (\LogicException $e) {
                // Thrown only after the whole wait: not yet an answer, and the
                // deadline decides. Anything else (a closed socket) is a failure.
                if ($e->getMessage() !== self::READ_TIMEOUT) {
                    throw $e;
                }
            }
        }

        return false;
    }
}
