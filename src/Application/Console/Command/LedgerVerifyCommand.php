<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Console\Command;

use Semitexa\Core\Attribute\AsCommand;
use Semitexa\Ledger\Application\Service\EnvelopeSeal;
use Semitexa\Ledger\Application\Service\LedgerConnection;
use Semitexa\Ledger\Domain\Model\LedgerEvent;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

/**
 * Walk the hash chain for every origin and verify all HMAC signatures.
 * Exits with a non-zero code if any inconsistency is found.
 *
 * Usage:
 *   bin/semitexa ledger:verify
 *   bin/semitexa ledger:verify --origin=store-a
 */
#[AsCommand(
    name: 'ledger:verify',
    description: 'Verify the SQLite ledger hash chain and HMAC signatures',
    options: [
        ['name' => 'origin', 'description' => 'Verify only this origin node'],
    ],
)]
final class LedgerVerifyCommand extends Command
{
    protected function configure(): void
    {
        $this->addOption('origin', null, InputOption::VALUE_OPTIONAL, 'Verify only this origin node');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io      = new SymfonyStyle($input, $output);
        $origin  = $input->getOption('origin');
        $nodeId  = (string) (getenv('LEDGER_NODE_ID') ?: 'node');
        $hmacKey = (string) (getenv('LEDGER_HMAC_KEY') ?: '');
        $dbPath  = (string) (getenv('LEDGER_DB_PATH') ?: "/var/lib/semitexa/ledger/{$nodeId}.sqlite");

        $db = new LedgerConnection($dbPath);

        $originFilter = $origin !== null ? 'WHERE origin_node = :origin' : '';
        $params       = $origin !== null ? ['origin' => $origin] : [];

        $origins = $db->fetchAll(
            "SELECT DISTINCT origin_node FROM events {$originFilter} ORDER BY origin_node",
            $params
        );

        if ($origins === []) {
            $io->success('Ledger is empty — nothing to verify.');
            return Command::SUCCESS;
        }

        $errors = 0;

        foreach ($origins as $row) {
            $originNode = $row['origin_node'];
            $io->writeln("Verifying origin: <info>{$originNode}</info>");

            $events = $db->fetchAll(
                'SELECT * FROM events WHERE origin_node = :origin ORDER BY sequence ASC',
                ['origin' => $originNode]
            );

            $prevHash = hash('sha256', "genesis:{$originNode}");

            foreach ($events as $row) {
                $event = LedgerEvent::fromRow($row);

                if ($event->prevHash !== $prevHash) {
                    $io->error(sprintf(
                        '  seq=%d event_id=%s — prev_hash mismatch (expected=%s stored=%s)',
                        $event->sequence, $event->eventId, $prevHash, $event->prevHash,
                    ));
                    $errors++;
                }

                if (!EnvelopeSeal::supports($event->hashVersion)) {
                    $io->error(sprintf(
                        '  seq=%d event_id=%s — unknown hash version %d',
                        $event->sequence, $event->eventId, $event->hashVersion,
                    ));
                    $errors++;
                    $prevHash = $event->hash;
                    continue;
                }

                // Each row under the formula of its own hash version: version 1
                // history stays verifiable beside version 2 events.
                if ($event->hash !== EnvelopeSeal::hashOf($event, $prevHash)) {
                    $io->error(sprintf(
                        '  seq=%d event_id=%s — hash mismatch',
                        $event->sequence, $event->eventId,
                    ));
                    $errors++;
                }

                if ($hmacKey !== ''
                    && !hash_equals(EnvelopeSeal::hmac($event->hashVersion, $event->hash, $hmacKey), $event->hmac)
                ) {
                    $io->error(sprintf(
                        '  seq=%d event_id=%s — HMAC mismatch (possible tampering)',
                        $event->sequence, $event->eventId,
                    ));
                    $errors++;
                }

                $prevHash = $event->hash;
            }

            if ($errors === 0) {
                $io->writeln(sprintf('  <info>✓</info> %d events OK', count($events)));
            }
        }

        if ($errors > 0) {
            $io->error("Verification failed with {$errors} error(s).");
            return Command::FAILURE;
        }

        $io->success('Ledger integrity verified — no issues found.');
        return Command::SUCCESS;
    }
}
