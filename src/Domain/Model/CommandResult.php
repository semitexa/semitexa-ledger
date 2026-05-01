<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Domain\Model;

/**
 * Result returned by CommandBus::sendAndWait() after the owner node
 * processes a command.
 */
final readonly class CommandResult
{
    public function __construct(
        public bool $success,
        public ?string $errorMessage = null,
        /** @var array<string, mixed> */
        public array $data = [],
    ) {}

    public static function ok(array $data = []): self
    {
        return new self(success: true, data: $data);
    }

    public static function error(string $message): self
    {
        return new self(success: false, errorMessage: $message);
    }

    public function toJson(): string
    {
        return json_encode([
            'success' => $this->success,
            'error'   => $this->errorMessage,
            'data'    => $this->data,
        ], JSON_THROW_ON_ERROR);
    }

    public static function fromJson(string $json): self
    {
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);

        return new self(
            success:      (bool) ($data['success'] ?? false),
            errorMessage: $data['error'] ?? null,
            data:         $data['data'] ?? [],
        );
    }
}
