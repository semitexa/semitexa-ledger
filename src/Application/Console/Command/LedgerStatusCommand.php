<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Console\Command;

use Semitexa\Core\Support\Row;
use Semitexa\Core\Attribute\AsCommand;
use Semitexa\Ledger\Application\Service\LedgerConnection;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * What this node's ledger holds and how far replication has got: events per
 * origin, what is still waiting to be published or applied, what was
 * quarantined, and where the stream consumer stands.
 *
 * Usage:
 *   bin/semitexa ledger:status
 *   bin/semitexa ledger:status --json
 *   bin/semitexa ledger:status --probe=<probe_id> --json   (is that probe here?)
 */
#[AsCommand(
    name: 'ledger:status',
    description: 'Show replication state of this node\'s ledger',
    options: [
        ['name' => 'json', 'description' => 'Print the status as JSON'],
        ['name' => 'probe', 'description' => 'Also report whether this ledger:probe id has arrived'],
    ],
)]
final class LedgerStatusCommand extends Command
{
    protected function configure(): void
    {
        $this->addOption('json', null, InputOption::VALUE_NONE, 'Print the status as JSON');
        $this->addOption('probe', null, InputOption::VALUE_REQUIRED, 'Also report whether this ledger:probe id has arrived');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $nodeId = (string) (getenv('LEDGER_NODE_ID') ?: '');
        if ($nodeId === '') {
            $output->writeln('<error>LEDGER_NODE_ID is not set: this node is not part of a ledger cluster.</error>');
            return Command::FAILURE;
        }

        $dbPath = (string) (getenv('LEDGER_DB_PATH') ?: "/var/lib/semitexa/ledger/{$nodeId}.sqlite");
        if (!is_file($dbPath)) {
            $output->writeln("<error>No ledger at {$dbPath} yet: the server has not booted the ledger on this node.</error>");
            return Command::FAILURE;
        }

        $status = self::collect(new LedgerConnection($dbPath), $nodeId, $input->getOption('probe'));

        if ($input->getOption('json') === true) {
            $output->writeln(json_encode($status, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES), OutputInterface::OUTPUT_RAW);
            return Command::SUCCESS;
        }

        $output->writeln("Node <info>{$nodeId}</info> — ledger {$dbPath}");
        $table = new Table($output);
        $table->setHeaders(['Origin', 'Source', 'Events', 'Last seq', 'Pending publish', 'Unapplied']);
        foreach ($status['origins'] as $row) {
            $table->addRow([$row['origin'], $row['source'], $row['events'], $row['last_sequence'], $row['pending_publish'], $row['unapplied']]);
        }
        $table->render();
        $output->writeln(sprintf('Quarantined (unresolved): %d', $status['quarantined']));
        foreach ($status['consumers'] as $consumer) {
            $output->writeln(sprintf(
                'Consumer %s on %s: stream position %d (updated %s)',
                $consumer['consumer'],
                $consumer['cluster'],
                $consumer['position'],
                $consumer['updated_at'],
            ));
        }
        if ($status['probe'] !== null) {
            $output->writeln(sprintf('Probe %s: %s', $status['probe']['id'], $status['probe']['arrived'] ? 'arrived' : 'not here'));
        }

        return Command::SUCCESS;
    }

    /**
     * @return array{
     *     node: string,
     *     origins: list<array{origin: string, source: string, events: int, last_sequence: int, pending_publish: int, unapplied: int}>,
     *     quarantined: int,
     *     consumers: list<array{consumer: string, cluster: string, position: int, updated_at: string}>,
     *     probe: array{id: string, arrived: bool, applied: bool}|null
     * }
     */
    public static function collect(LedgerConnection $db, string $nodeId, mixed $probeId = null): array
    {
        $origins = [];
        foreach ($db->fetchAll(
            "SELECT origin_node, source, COUNT(*) AS events, MAX(sequence) AS last_sequence,
                    SUM(CASE WHEN source = 'local' AND publish_status = 'pending' THEN 1 ELSE 0 END) AS pending_publish,
                    SUM(CASE WHEN source = 'remote' AND applied_at IS NULL THEN 1 ELSE 0 END) AS unapplied
             FROM events GROUP BY origin_node, source ORDER BY origin_node"
        ) as $row) {
            $r = Row::of($row);
            $origins[] = [
                'origin'          => $r->string('origin_node'),
                'source'          => $r->string('source'),
                'events'          => $r->int('events'),
                'last_sequence'   => $r->int('last_sequence'),
                'pending_publish' => $r->int('pending_publish'),
                'unapplied'       => $r->int('unapplied'),
            ];
        }

        $consumers = [];
        foreach ($db->fetchAll('SELECT consumer_id, cluster_id, last_nats_sequence, updated_at FROM consumer_state ORDER BY cluster_id') as $row) {
            $r = Row::of($row);
            $consumers[] = [
                'consumer'   => $r->string('consumer_id'),
                'cluster'    => $r->string('cluster_id'),
                'position'   => $r->int('last_nats_sequence'),
                'updated_at' => $r->string('updated_at'),
            ];
        }

        $probe = null;
        if (is_string($probeId) && $probeId !== '') {
            $row = $db->fetchOne(
                "SELECT applied_at, source FROM events
                 WHERE domain = 'ledger' AND event_type = 'ledger_probe_recorded'
                   AND json_extract(payload, '$.probeId') = :probe",
                ['probe' => $probeId]
            );
            $probe = [
                'id'      => $probeId,
                'arrived' => $row !== null,
                'applied' => $row !== null && ($row['source'] === 'local' || $row['applied_at'] !== null),
            ];
        }

        return [
            'node'        => $nodeId,
            'origins'     => $origins,
            'quarantined' => Row::asInt($db->fetchScalar('SELECT COUNT(*) FROM quarantined_events WHERE resolved_at IS NULL')),
            'consumers'   => $consumers,
            'probe'       => $probe,
        ];
    }
}
