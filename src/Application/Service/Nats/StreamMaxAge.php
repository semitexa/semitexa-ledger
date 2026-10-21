<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Application\Service\Nats;

/**
 * Gives an existing stream a max age. NatsClient::ensureStream() only
 * creates, and a stream made before a retention was set kept every message.
 */
final class StreamMaxAge
{
    /**
     * $maxAgeNanoSeconds 0 keeps every message. The update sends back the
     * configuration the server reports, with only max_age changed: the
     * library's own Configuration drops every field it does not know
     * (max_msgs, compression, allow_direct, ...), and an update without them
     * would reset those to their defaults.
     *
     * @return bool whether the stream was changed
     */
    public static function ensure(NatsClient $client, string $streamName, int $maxAgeNanoSeconds): bool
    {
        $stream = $client->jetStream()->getStream($streamName);
        if (!$stream->exists()) {
            return false;
        }
        // The reply's fields are read through a magic getter: isset() on
        // them answers false, so read the field and check what came back.
        $info = $stream->info();
        $current = is_object($info) ? $info->config : null;
        if (!$current instanceof \stdClass || (int) ($current->max_age ?? 0) === $maxAgeNanoSeconds) {
            return false;
        }
        $config = (array) $current;
        $config['max_age'] = $maxAgeNanoSeconds;
        $stream->client->api('STREAM.UPDATE.' . $streamName, $config);

        return true;
    }
}
