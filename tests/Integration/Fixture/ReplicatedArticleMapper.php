<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Integration\Fixture;

use Semitexa\Orm\Attribute\AsMapper;
use Semitexa\Orm\Domain\Contract\ResourceModelMapperInterface;

#[AsMapper(resourceModel: ReplicatedArticleResourceModel::class, domainModel: ReplicatedArticle::class)]
final class ReplicatedArticleMapper implements ResourceModelMapperInterface
{
    public function toDomain(object $resourceModel): object
    {
        $resourceModel instanceof ReplicatedArticleResourceModel || throw new \InvalidArgumentException('Unexpected resource model.');

        return new ReplicatedArticle($resourceModel->id, $resourceModel->title, $resourceModel->body);
    }

    public function toSourceModel(object $domainModel): object
    {
        $domainModel instanceof ReplicatedArticle || throw new \InvalidArgumentException('Unexpected domain model.');

        return new ReplicatedArticleResourceModel($domainModel->id, $domainModel->title, $domainModel->body);
    }
}
