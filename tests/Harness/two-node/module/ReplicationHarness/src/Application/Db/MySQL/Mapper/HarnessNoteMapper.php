<?php

declare(strict_types=1);

namespace Semitexa\Modules\ReplicationHarness\Application\Db\MySQL\Mapper;

use Semitexa\Modules\ReplicationHarness\Application\Db\MySQL\Model\HarnessNoteResource;
use Semitexa\Modules\ReplicationHarness\Domain\Model\HarnessNote;
use Semitexa\Orm\Attribute\AsMapper;
use Semitexa\Orm\Domain\Contract\ResourceModelMapperInterface;

#[AsMapper(resourceModel: HarnessNoteResource::class, domainModel: HarnessNote::class)]
final class HarnessNoteMapper implements ResourceModelMapperInterface
{
    public function toDomain(object $resourceModel): object
    {
        $resourceModel instanceof HarnessNoteResource || throw new \InvalidArgumentException('Unexpected resource model.');

        return new HarnessNote($resourceModel->id, $resourceModel->title, $resourceModel->body);
    }

    public function toSourceModel(object $domainModel): object
    {
        $domainModel instanceof HarnessNote || throw new \InvalidArgumentException('Unexpected domain model.');

        return new HarnessNoteResource($domainModel->id, $domainModel->title, $domainModel->body);
    }
}
