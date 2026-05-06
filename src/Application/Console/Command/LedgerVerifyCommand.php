<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Console\Command;

use Semitexa\Core\Attribute\AsCommand;
use Semitexa\Ledger\Application\Service\LedgerConnection;
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
                'SELECT event_id, sequence, payload, hash, prev_hash, hmac
                 FROM events WHERE origin_node = :origin ORDER BY sequence ASC',
                ['origin' => $originNode]
            );

            $prevHash = hash('sha256', "genesis:{$originNode}");

            foreach ($events as $event) {
                $seq       = (int) $event['sequence'];
                $eventId   = $event['event_id'];
                $payload   = $event['payload'];
                $hash      = $event['hash'];
                $storedPrev = $event['prev_hash'];
                $hmac      = $event['hmac'];

                if ($storedPrev !== $prevHash) {
                    $io->error(sprintf(
                        '  seq=%d event_id=%s — prev_hash mismatch (expected=%s stored=%s)',
                        $seq, $eventId, $prevHash, $storedPrev,
                    ));
                    $errors++;
                }

                $payloadArray = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
                $expectedHash = hash('sha256', $prevHash . $eventId . json_encode($payloadArray, JSON_THROW_ON_ERROR));

                if ($hash !== $expectedHash) {
                    $io->error(sprintf(
                        '  seq=%d event_id=%s — hash mismatch',
                        $seq, $eventId,
                    ));
                    $errors++;
                }

                if ($hmacKey !== '') {
                    $expectedHmac = hash_hmac('sha256', $hash, $hmacKey);
                    if (!hash_equals($expectedHmac, $hmac)) {
                        $io->error(sprintf(
                            '  seq=%d event_id=%s — HMAC mismatch (possible tampering)',
                            $seq, $eventId,
                        ));
                        $errors++;
                    }
                }

                $prevHash = $hash;
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
