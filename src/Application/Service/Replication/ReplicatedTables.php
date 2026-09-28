<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Replication;

use Semitexa\Core\Discovery\ClassDiscovery;
use Semitexa\Orm\Attribute\Replicated;
use Semitexa\Orm\Metadata\ResourceModelMetadataRegistry;

/**
 * The tables this node's code marks #[Replicated], with their primary-key
 * columns — the only tables a replicated change from another node may touch.
 */
final class ReplicatedTables
{
    /**
     * @return array<string, string> table => primary-key column
     */
    public static function discover(ClassDiscovery $discovery): array
    {
        $registry = new ResourceModelMetadataRegistry();
        $tables   = [];

        foreach ($discovery->findClassesWithAttribute(Replicated::class) as $class) {
            if (!class_exists($class)) {
                continue;
            }
            $metadata = $registry->for($class);
            if ($metadata->primaryKeyProperty === null) {
                continue;
            }
            $tables[$metadata->tableName] = $metadata->column($metadata->primaryKeyProperty)->columnName;
        }

        ksort($tables);

        return $tables;
    }
}
