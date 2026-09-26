<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Unit;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Semitexa\Ledger\Application\Service\Nats\NatsClient;
use Semitexa\Ledger\Domain\Model\ClusterConfig;

/**
 * Basis\Nats\Client::request() waits for its reply via
 * process($this->configuration->timeout) — a single blocking read bounded by
 * the CLIENT'S CONFIGURATION timeout (fixed at construction, defaults to 1.0s
 * — see Basis\Nats\Configuration::$timeout), not by the stream timeout that
 * Connection::setTimeout() adjusts. NatsClient::request() used to call only
 * setTimeout(), so a caller asking for e.g. a 2s timeout still got the
 * RuntimeException after ~1s — the configuration timeout NatsClient never
 * touched.
 *
 * This is exercised end-to-end against a minimal fake NATS server (forked
 * child process) that completes the INFO/CONNECT handshake and then never
 * answers the request, so the only thing bounding how long request() blocks
 * is the timeout logic under test.
 */
final class NatsClientRequestTimeoutTest extends TestCase
{
    private $server;
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
    public function request_honors_the_caller_supplied_timeout_not_just_the_1s_configuration_default(): void
    {
        if (!extension_loaded('pcntl') || !extension_loaded('posix')) {
            self::markTestSkipped('pcntl/posix required to run the fake NATS server for this test');
        }

        $port = $this->startFakeNatsServerThatNeverReplies();

        $client = new NatsClient(new ClusterConfig(id: 'test', url: "nats://127.0.0.1:{$port}"));

        $requestedTimeout = 2.0;
        $start = microtime(true);

        try {
            $client->request('some.subject', 'payload', $requestedTimeout);
            self::fail('expected a RuntimeException — the fake server never replies');
        } catch (\RuntimeException $e) {
            $elapsed = microtime(true) - $start;

            self::assertStringContainsString('timed out after 2s', $e->getMessage());

            // Before the fix this consistently returned after ~1.0s (the
            // Configuration default), regardless of $requestedTimeout. It must
            // instead wait close to the full 2s that was asked for.
            self::assertGreaterThanOrEqual(
                1.5,
                $elapsed,
                "request() returned after only {$elapsed}s — it must honor the requested {$requestedTimeout}s timeout, "
                    . 'not the ~1s Basis\Nats\Configuration default',
            );
            self::assertLessThan(3.0, $elapsed, 'must not wait dramatically longer than the requested timeout either');
        }
    }

    #[Test]
    public function a_connection_failure_during_setup_restores_the_configuration_timeout(): void
    {
        // Reserve a port, then release it so nothing listens there: the lazy
        // connect inside setTimeout() is refused immediately.
        $probe = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if ($probe === false) {
            self::fail("could not reserve a port: {$errstr}");
        }
        $name = (string) stream_socket_get_name($probe, false);
        $port = (int) substr((string) strrchr($name, ':'), 1);
        fclose($probe);

        $client = new NatsClient(new ClusterConfig(id: 'test', url: "nats://127.0.0.1:{$port}"));
        $inner = (new \ReflectionProperty(NatsClient::class, 'client'))->getValue($client);
        self::assertInstanceOf(\Basis\Nats\Client::class, $inner);
        $before = $inner->configuration->timeout;

        $refused = false;
        try {
            $client->request('some.subject', 'payload', $before + 4.0);
        } catch (\Throwable) {
            // expected: nothing listens on the port
            $refused = true;
        }
        self::assertTrue($refused, 'expected the connection to be refused');

        self::assertSame(
            $before,
            $inner->configuration->timeout,
            'a failed connection setup must not leave the request timeout on the shared configuration',
        );
    }

    /**
     * A minimal NATS server: accepts one connection, sends a bare INFO
     * message to satisfy Connection::init()'s handshake, then holds the
     * socket open without ever sending a reply. Runs in a forked child so the
     * parent (this test) can synchronously drive NatsClient::request()
     * against it.
     */
    private function startFakeNatsServerThatNeverReplies(): int
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if ($server === false) {
            self::fail("could not start fake NATS server: {$errstr}");
        }
        $this->server = $server;

        $name = stream_socket_get_name($server, false);
        $port = (int) substr((string) strrchr($name, ':'), 1);

        $pid = pcntl_fork();
        if ($pid === -1) {
            self::fail('pcntl_fork() failed');
        }

        if ($pid === 0) {
            // Child: speak just enough NATS to let the client connect, then
            // go silent for long enough to outlast the test's timeout budget.
            $conn = @stream_socket_accept($server, 5);
            if ($conn !== false) {
                fwrite($conn, "INFO {}\r\n");
                usleep(4_000_000);
                fclose($conn);
            }
            // Never exit() here: it would run the forked PHPUnit's shutdown
            // handlers (result printing, junit) a second time.
            posix_kill(posix_getpid(), SIGKILL);
        }

        $this->childPid = $pid;

        return $port;
    }
}
