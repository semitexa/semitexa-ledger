<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Console;

use Semitexa\Core\Attribute\AsCommand;
use Semitexa\Core\Container\ContainerFactory;
use Semitexa\Ledger\Ledger\LedgerConnection;
use Semitexa\Ledger\Ledger\ReplayHandlerRegistry;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

/**
 * Replay ledger events to the main database.
 *
 * Useful for:
 *  - Recovery after main DB restore from backup.
 *  - Applying new projections to historical events.
 *  - Debugging replay handler behaviour.
 *
 * Usage:
 *   bin/semitexa ledger:replay
 *   bin/semitexa ledger:replay --since="2026-04-10T10:00:00Z"
 *   bin/semitexa ledger:replay --domain=inventory --dry-run
 *   bin/semitexa ledger:replay --event-type=stock_adjusted --origin=store-a
 */
#[AsCommand(
    name: 'ledger:replay',
    description: 'Replay ledger events to the main database (recovery / projection rebuild)',
    options: [
        ['name' => 'since',       'description' => 'ISO 8601 timestamp to replay from (default: unapplied events)'],
        ['name' => 'domain',      'description' => 'Filter by domain (e.g. inventory)'],
        ['name' => 'event-type',  'description' => 'Filter by event type (e.g. stock_adjusted)'],
        ['name' => 'origin',      'description' => 'Filter by origin node'],
        ['name' => 'dry-run',     'description' => 'Print what would be replayed without applying'],
    ],
)]
final class LedgerReplayCommand extends Command
{
    protected function configure(): void
    {
        $this->addOption('since',      null, InputOption::VALUE_OPTIONAL, 'Replay from this ISO 8601 timestamp');
        $this->addOption('domain',     null, InputOption::VALUE_OPTIONAL, 'Filter by domain');
        $this->addOption('event-type', null, InputOption::VALUE_OPTIONAL, 'Filter by event type');
        $this->addOption('origin',     null, InputOption::VALUE_OPTIONAL, 'Filter by origin node');
        $this->addOption('dry-run',    null, InputOption::VALUE_NONE,     'Print without applying');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io      = new SymfonyStyle($input, $output);
        $dryRun  = (bool) $input->getOption('dry-run');
        $since   = $input->getOption('since');
        $domain  = $input->getOption('domain');
        $evtType = $input->getOption('event-type');
        $origin  = $input->getOption('origin');

        $nodeId  = (string) (getenv('LEDGER_NODE_ID') ?: 'node');
        $hmacKey = (string) (getenv('LEDGER_HMAC_KEY') ?: '');
        $dbPath  = (string) (getenv('LEDGER_DB_PATH') ?: "/var/lib/semitexa/ledger/{$nodeId}.sqlite");

        $db       = new LedgerConnection($dbPath);
        $registry = ContainerFactory::get()->resolve(ReplayHandlerRegistry::class);

        [$sql, $params] = $this->buildQuery($since, $domain, $evtType, $origin);

        $rows = $db->fetchAll($sql, $params);

        if ($rows === []) {
            $io->success('No events to replay.');
            return Command::SUCCESS;
        }

        $io->note(sprintf('Found %d event(s) to replay.%s', count($rows), $dryRun ? ' (dry-run)' : ''));

        $applied = 0;
        $container = ContainerFactory::get();

        foreach ($rows as $row) {
            $event = \Semitexa\Ledger\Dto\LedgerEvent::fromRow($row);

            if ($dryRun) {
                $io->writeln(sprintf(
                    '  [%s] %s.%s seq=%d origin=%s',
                    $event->createdAt,
                    $event->domain,
                    $event->eventType,
                    $event->sequence,
                    $event->originNode,
                ));
                continue;
            }

            $registry->apply($event, fn (string $class) => $container->resolve($class));

            $db->execute(
                'UPDATE events SET applied_at = :now WHERE event_id = :id',
                ['now' => gmdate('Y-m-d\TH:i:s\Z'), 'id' => $event->eventId]
            );

            $applied++;
        }

        if (!$dryRun) {
            $io->success("Replayed {$applied} event(s).");
        }

        return Command::SUCCESS;
    }

    private function buildQuery(?string $since, ?string $domain, ?string $evtType, ?string $origin): array
    {
        $conditions = [];
        $params     = [];

        if ($since !== null) {
            $conditions[] = 'created_at >= :since';
            $params['since'] = $since;
        } else {
            $conditions[] = 'applied_at IS NULL';
        }

        if ($domain !== null) {
            $conditions[] = 'domain = :domain';
            $params['domain'] = $domain;
        }

        if ($evtType !== null) {
            $conditions[] = 'event_type = :event_type';
            $params['event_type'] = $evtType;
        }

        if ($origin !== null) {
            $conditions[] = 'origin_node = :origin';
            $params['origin'] = $origin;
        }

        $where = $conditions !== [] ? 'WHERE ' . implode(' AND ', $conditions) : '';
        $sql   = "SELECT * FROM events {$where} ORDER BY origin_node, sequence ASC";

        return [$sql, $params];
    }
}
