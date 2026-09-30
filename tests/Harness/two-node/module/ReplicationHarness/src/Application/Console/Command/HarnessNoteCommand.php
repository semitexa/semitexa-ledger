<?php

declare(strict_types=1);

namespace Semitexa\Modules\ReplicationHarness\Application\Console\Command;

use Semitexa\Core\Attribute\AsCommand;
use Semitexa\Core\Attribute\InjectAsReadonly;
use Semitexa\Core\Console\BaseCommand;
use Semitexa\Ledger\Application\Service\Replication\ReplicationConflicts;
use Semitexa\Modules\ReplicationHarness\Application\Db\MySQL\Mapper\HarnessNoteMapper;
use Semitexa\Modules\ReplicationHarness\Application\Handler\DomainListener\HarnessConflictListener;
use Semitexa\Modules\ReplicationHarness\Application\Db\MySQL\Model\HarnessNoteResource;
use Semitexa\Modules\ReplicationHarness\Domain\Model\HarnessNote;
use Semitexa\Orm\Application\Service\Connection\ConnectionRegistry;
use Semitexa\Orm\Application\Service\Mapping\MapperRegistry;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Writes harness notes through the ORM write engine — the path real
 * application code takes, so the capture runs exactly as it would.
 *
 *   harness:note create --id=<uuid> --title=T --body=B
 *   harness:note set    --id=<uuid> --field=title --value=V
 *   harness:note delete --id=<uuid>
 *   harness:note dump                     (all rows, JSON, sorted)
 *   harness:note conflicts                (journaled + announced conflicts, JSON)
 *   harness:note retry  --id=<uuid>       (retry this row's open conflicts, as application code would after resolving)
 */
#[AsCommand(name: 'harness:note', description: 'Two-node harness: write or dump replicated notes through the ORM')]
final class HarnessNoteCommand extends BaseCommand
{
    #[InjectAsReadonly]
    protected ConnectionRegistry $connections;

    #[InjectAsReadonly]
    protected ReplicationConflicts $conflicts;

    protected function configure(): void
    {
        $this->addArgument('action', InputArgument::REQUIRED, 'create | set | delete | dump | conflicts | retry');
        $this->addOption('id', null, InputOption::VALUE_REQUIRED);
        $this->addOption('title', null, InputOption::VALUE_REQUIRED, '', '');
        $this->addOption('body', null, InputOption::VALUE_REQUIRED, '', '');
        $this->addOption('field', null, InputOption::VALUE_REQUIRED);
        $this->addOption('value', null, InputOption::VALUE_REQUIRED, '', '');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $orm     = $this->connections->manager('default');
        $engine  = $orm->getAggregateWriteEngine();
        $mappers = new MapperRegistry();
        $mappers->build(mapperClasses: [HarnessNoteMapper::class], domainModelClasses: [HarnessNote::class]);
        $id      = (string) $input->getOption('id');

        switch ((string) $input->getArgument('action')) {
            case 'create':
                $engine->insert(
                    new HarnessNote($id, (string) $input->getOption('title'), (string) $input->getOption('body')),
                    HarnessNoteResource::class,
                    $mappers,
                );
                break;

            case 'set':
                $current = $this->find($orm, $id) ?? throw new \RuntimeException("No note {$id} here.");
                $field   = (string) $input->getOption('field');
                $value   = (string) $input->getOption('value');
                $engine->update(
                    new HarnessNote(
                        $id,
                        $field === 'title' ? $value : $current['title'],
                        $field === 'body' ? $value : $current['body'],
                    ),
                    HarnessNoteResource::class,
                    $mappers,
                );
                break;

            case 'delete':
                $current = $this->find($orm, $id) ?? throw new \RuntimeException("No note {$id} here.");
                $engine->delete(new HarnessNote($id, $current['title'], $current['body']), HarnessNoteResource::class, $mappers);
                break;

            case 'dump':
                $rows = $orm->getAdapter()->execute('SELECT id, title, body FROM harness_notes ORDER BY id')->rows;
                $output->writeln(json_encode($rows, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE));
                break;

            case 'conflicts':
                $journaled = $orm->getAdapter()->execute(
                    'SELECT table_name, row_pk, columns, resolved_at FROM replication_conflict ORDER BY table_name, row_pk',
                )->rows;
                $announced = is_file(HarnessConflictListener::LOG)
                    ? array_values(array_filter(explode("\n", (string) file_get_contents(HarnessConflictListener::LOG))))
                    : [];
                $output->writeln(json_encode([
                    'journaled' => array_map(static fn (array $r): array => [
                        'table'   => (string) $r['table_name'],
                        'row'     => (string) $r['row_pk'],
                        'columns' => json_decode((string) $r['columns'], true, 8, JSON_THROW_ON_ERROR),
                        'open'    => $r['resolved_at'] === null,
                    ], $journaled),
                    'announced' => array_map(static fn (string $line): mixed => json_decode($line, true, 8, JSON_THROW_ON_ERROR), $announced),
                ], JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE));
                break;

            case 'retry':
                $keys = $orm->getAdapter()->execute(
                    'SELECT conflict_key FROM replication_conflict WHERE row_pk = :pk AND resolved_at IS NULL',
                    ['pk' => $id],
                )->rows;
                $outcomes = [];
                foreach ($keys as $row) {
                    $outcomes[] = $this->conflicts->retry((string) $row['conflict_key'])->value;
                }
                $output->writeln(json_encode($outcomes, JSON_THROW_ON_ERROR));
                break;

            default:
                $output->writeln('<error>Unknown action.</error>');
                return Command::FAILURE;
        }

        return Command::SUCCESS;
    }

    /** @return array{title: string, body: string}|null */
    private function find(\Semitexa\Orm\OrmManager $orm, string $id): ?array
    {
        $row = $orm->getAdapter()->execute('SELECT title, body FROM harness_notes WHERE id = :id', ['id' => $id])->rows[0] ?? null;

        return $row === null ? null : ['title' => (string) $row['title'], 'body' => (string) $row['body']];
    }
}
