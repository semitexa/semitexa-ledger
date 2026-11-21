<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Unit;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Ledger\Application\Service\Nats\NatsClient;
use Semitexa\Ledger\Domain\Model\ClusterConfig;

/**
 * After 10 s of empty pulls NatsClient proves the connection with a PING.
 * Basis\Nats\Connection::ping() sends it and reads exactly ONE frame, for at
 * most the configuration timeout (1 s): a frame that arrives ahead of the PONG
 * (the pull's own "408 Request Timeout" sent just after the client stopped
 * reading, a late reply, an INFO) or a PONG a busy broker sends after 1 s made
 * it return false or, on a connection whose last PONG is over 2 s old, throw
 * "Socket read timeout": a healthy connection was dropped (release preflight,
 * 2026-11-03).
 *
 * Run against a scripted NATS server in a forked child: it completes the
 * handshake, never delivers, and answers the PING the way each test names.
 */
final class NatsClientLivenessPingTest extends TestCase
{
    /** @var resource|null */
    private $server = null;
    private ?int $childPid = null;

    protected function tearDown(): void
    {
        if ($this->childPid !== null) {
            posix_kill($this->childPid, SIGKILL);
            pcntl_waitpid($this->childPid, $status);
            $this->childPid = null;
        }
        if (is_resource($this->server)) {
            fclose($this->server);
        }
    }

    #[Test]
    public function a_frame_that_arrives_ahead_of_the_pong_does_not_drop_a_healthy_connection(): void
    {
        $client = $this->clientDueForALivenessCheck('stray');
        $connection = $this->innerClient($client);

        $start = microtime(true);
        self::assertSame([], $client->pullMessages('S', 'C', 10, 0.1));

        self::assertLessThan(1.5, microtime(true) - $start, 'the PONG was right behind the frame: no waiting for it');
        self::assertSame($connection, $this->innerClient($client), 'the connection is kept');
    }

    #[Test]
    public function a_pong_later_than_the_read_timeout_does_not_drop_a_healthy_connection(): void
    {
        $client = $this->clientDueForALivenessCheck('late');
        $connection = $this->innerClient($client);

        self::assertSame([], $client->pullMessages('S', 'C', 10, 0.1));
        self::assertSame($connection, $this->innerClient($client), 'the connection is kept');

        // The late PONG was read by the check that waited for it: nothing of
        // it is left on the socket to cut the next pull short.
        $start = microtime(true);
        self::assertSame([], $client->pullMessages('S', 'C', 10, 0.1));
        self::assertGreaterThanOrEqual(0.5, microtime(true) - $start, 'the next pull waited its whole window');
    }

    #[Test]
    public function a_connection_that_never_answers_is_still_declared_dead_within_three_seconds_of_the_ping(): void
    {
        $client = $this->clientDueForALivenessCheck('dead');
        $connection = $this->innerClient($client);

        $start = microtime(true);
        try {
            $client->pullMessages('S', 'C', 10, 0.1);
            self::fail('a connection that never answers a PING must be declared dead');
        } catch (\RuntimeException $e) {
            self::assertSame('NATS connection did not answer PING; reconnecting.', $e->getMessage());
        }

        // 0.6 s of pull, then the PING and 3 s for its PONG.
        self::assertLessThan(4.5, microtime(true) - $start, 'declared dead within the bound');
        self::assertNotSame($connection, $this->innerClientOrNull($client), 'the dead connection is dropped');
    }

    /** A connected client whose last sign of life is older than LIVENESS_AFTER, so its next empty pull pings. */
    private function clientDueForALivenessCheck(string $script): NatsClient
    {
        $port = $this->startScriptedNatsServer($script);
        $client = new NatsClient(new ClusterConfig(id: 'test', url: "nats://127.0.0.1:{$port}"));
        // As LedgerBootstrap does for every cluster: a PING/PONG, which dates
        // the library's last PONG. Once that is older than its pingInterval
        // (2 s) a read that times out THROWS "Socket read timeout" instead of
        // returning: the state every long-lived connection is in.
        $client->connect();
        usleep(2_100_000);
        (new \ReflectionProperty(NatsClient::class, 'aliveAt'))->setValue($client, microtime(true) - 60.0);

        return $client;
    }

    private function innerClient(NatsClient $client): \Basis\Nats\Client
    {
        $inner = $this->innerClientOrNull($client);
        self::assertInstanceOf(\Basis\Nats\Client::class, $inner);

        return $inner;
    }

    private function innerClientOrNull(NatsClient $client): ?\Basis\Nats\Client
    {
        $inner = (new \ReflectionProperty(NatsClient::class, 'client'))->getValue($client);

        return $inner instanceof \Basis\Nats\Client ? $inner : null;
    }

    /**
     * Accepts one connection, sends INFO, delivers nothing, answers the first
     * PING (connect()) at once and the SECOND, the liveness check, by its
     * script (every later one at once):
     *  - 'stray': the pull's "408 Request Timeout" status frame, then the PONG;
     *  - 'late':  the PONG after 1.3 s, past the client's 1 s read timeout;
     *  - 'dead':  nothing, ever.
     */
    private function startScriptedNatsServer(string $script): int
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if ($server === false) {
            self::fail("could not start the scripted NATS server: {$errstr}");
        }
        $this->server = $server;

        $name = (string) stream_socket_get_name($server, false);
        $port = (int) substr((string) strrchr($name, ':'), 1);

        $pid = pcntl_fork();
        if ($pid === -1) {
            self::fail('pcntl_fork() failed');
        }

        if ($pid === 0) {
            $conn = @stream_socket_accept($server, 5);
            if ($conn !== false) {
                fwrite($conn, "INFO {}\r\n");
                $subject = 'handler.none';
                $sid = '0';
                $pings = 0;
                while (($line = fgets($conn)) !== false) {
                    $line = rtrim($line, "\r\n");
                    if (str_starts_with($line, 'SUB ')) {
                        [, $subject, $sid] = explode(' ', $line) + [2 => '0'];
                        continue;
                    }
                    if ($line !== 'PING') {
                        continue;
                    }
                    if (++$pings !== 2) {
                        fwrite($conn, "PONG\r\n");
                        continue;
                    }
                    if ($script === 'stray') {
                        $headers = "NATS/1.0 408 Request Timeout\r\n\r\n";
                        $length = strlen($headers);
                        fwrite($conn, "HMSG {$subject} {$sid} {$length} {$length}\r\n{$headers}\r\nPONG\r\n");
                    } elseif ($script === 'late') {
                        usleep(1_300_000);
                        fwrite($conn, "PONG\r\n");
                    }
                }
            }
            // Never exit() here: it would run the forked PHPUnit's shutdown
            // handlers (result printing, junit) a second time.
            posix_kill(posix_getpid(), SIGKILL);
        }

        $this->childPid = $pid;

        return $port;
    }
}
