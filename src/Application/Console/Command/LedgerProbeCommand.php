<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Console\Command;

use Semitexa\Core\Attribute\AsCommand;
use Semitexa\Core\Attribute\InjectAsReadonly;
use Semitexa\Core\Console\BaseCommand;
use Semitexa\Ledger\Application\Payload\Event\LedgerProbeRecorded;
use Semitexa\Ledger\Application\Service\AggregateOwnershipService;
use Semitexa\Ledger\Application\Service\LedgerConnection;
use Semitexa\Ledger\Application\Service\LedgerSchema;
use Semitexa\Ledger\Application\Service\LedgerWriter;
use Semitexa\Ledger\Application\Service\OwnershipCache;
use Semitexa\Ledger\Application\Service\UuidV7;
use Semitexa\Orm\Application\Service\Connection\ConnectionRegistry;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Append a probe event to this node's ledger, to check replication end to end.
 *
 * The command only writes the SQLite ledger. The running server's publisher
 * picks the row up like any other pending event, so a probe that shows up on
 * a peer (`ledger:status` there) has crossed the whole path: this node's
 * ledger, its publisher, the stream, the peer's replayer and the peer's
 * ledger. Run it on a node whose server is up.
 *
 * Usage:
 *   bin/semitexa ledger:probe
 *   bin/semitexa ledger:probe --note="after deploy" --json
 */
#[AsCommand(
    name: 'ledger:probe',
    description: 'Append a probe event to this node\'s ledger to check replication to its peers',
    options: [
        ['name' => 'note', 'description' => 'Free text carried by the probe'],
        ['name' => 'json', 'description' => 'Print the probe as JSON'],
    ],
)]
final class LedgerProbeCommand extends BaseCommand
{
    #[InjectAsReadonly]
    protected ConnectionRegistry $connections;

    protected function configure(): void
    {
        $this->addOption('note', null, InputOption::VALUE_REQUIRED, 'Free text carried by the probe', '');
        $this->addOption('json', null, InputOption::VALUE_NONE, 'Print the probe as JSON');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $nodeId  = (string) (getenv('LEDGER_NODE_ID') ?: '');
        $hmacKey = (string) (getenv('LEDGER_HMAC_KEY') ?: '');
        if ($nodeId === '' || $hmacKey === '') {
            $output->writeln('<error>LEDGER_NODE_ID and LEDGER_HMAC_KEY must be set: this node is not part of a ledger cluster.</error>');
            return Command::FAILURE;
        }

        $dbPath = (string) (getenv('LEDGER_DB_PATH') ?: "/var/lib/semitexa/ledger/{$nodeId}.sqlite");
        $dir = dirname($dbPath);
        if (!is_dir($dir) && !mkdir($dir, 0755, true) && !is_dir($dir)) {
            $output->writeln("<error>Could not create ledger directory: {$dir}</error>");
            return Command::FAILURE;
        }

        $db = new LedgerConnection($dbPath);
        (new LedgerSchema($db, $nodeId))->migrate();

        $ownership = new AggregateOwnershipService(
            $nodeId,
            $this->connections->manager((string) (getenv('LEDGER_DB_CONNECTION') ?: 'default'))->getAdapter(),
            new OwnershipCache(),
        );

        $probeId = UuidV7::generate();
        $note    = (string) $input->getOption('note');
        $event   = (new LedgerWriter($db, $nodeId, $hmacKey, $ownership))
            ->append(LedgerProbeRecorded::of($probeId, $note));

        if ($event === null) {
            $output->writeln('<error>The probe was not written: LedgerProbeRecorded is not #[Propagated].</error>');
            return Command::FAILURE;
        }

        if ($input->getOption('json') === true) {
            $output->writeln(json_encode([
                'probe_id' => $probeId,
                'event_id' => $event->eventId,
                'origin'   => $nodeId,
                'sequence' => $event->sequence,
            ], JSON_THROW_ON_ERROR));
            return Command::SUCCESS;
        }

        $output->writeln(sprintf(
            'Probe <info>%s</info> written on <info>%s</info> at sequence %d. Check a peer with: bin/semitexa ledger:status',
            $probeId,
            $nodeId,
            $event->sequence,
        ));

        return Command::SUCCESS;
    }
}
