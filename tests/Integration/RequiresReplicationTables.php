<?php

declare(strict_types=1);

namespace Semitexa\Ledger\Tests\Integration;

use Semitexa\Orm\Adapter\DatabaseAdapterInterface;
use Semitexa\Orm\OrmManager;

/**
 * A MySQL connection with the replication tables and the fixture table in
 * place — or the test is skipped. Each test that needs them creates all of
 * them, so none depends on another having run first.
 */
trait RequiresReplicationTables
{
    private const ARTICLES = 'ledger_it_articles';

    private OrmManager $orm;
    private DatabaseAdapterInterface $db;

    private function connectWithReplicationTables(): void
    {
        try {
            $this->orm = new OrmManager();
            if ($this->orm->getDriver() !== 'mysql') {
                throw new \RuntimeException('replication needs MySQL (named upserts, locking reads)');
            }
            $this->db = $this->orm->getAdapter();
            $this->db->execute('SELECT 1');
        } catch (\Throwable $e) {
            self::markTestSkipped('MySQL unavailable: ' . $e->getMessage());
        }

        $this->db->execute('CREATE TABLE IF NOT EXISTS ' . self::ARTICLES . ' (id CHAR(36) PRIMARY KEY, title VARCHAR(255) NOT NULL, body VARCHAR(255) NOT NULL)');
        $this->db->execute(
            'CREATE TABLE IF NOT EXISTS replication_field_clock (id BIGINT AUTO_INCREMENT PRIMARY KEY, table_name VARCHAR(64) NOT NULL, '
            . 'row_pk VARCHAR(191) NOT NULL, column_name VARCHAR(64) NOT NULL, hlc CHAR(20) NOT NULL, node VARCHAR(64) NOT NULL, '
            . 'UNIQUE KEY uniq_replication_field_clock (table_name, row_pk, column_name))'
        );
        $this->db->execute(
            'CREATE TABLE IF NOT EXISTS replication_outbox (id BIGINT AUTO_INCREMENT PRIMARY KEY, event_id CHAR(36) NOT NULL, '
            . 'payload LONGTEXT NOT NULL, created_at DATETIME NOT NULL, UNIQUE KEY uniq_replication_outbox_event (event_id))'
        );
        $this->db->execute(
            'CREATE TABLE IF NOT EXISTS replication_outbox_dead (id BIGINT AUTO_INCREMENT PRIMARY KEY, event_id VARCHAR(36) NOT NULL, '
            . 'payload LONGTEXT NOT NULL, error VARCHAR(500) NOT NULL, failed_at DATETIME NOT NULL, UNIQUE KEY uniq_replication_outbox_dead_event (event_id))'
        );
        $this->db->execute(
            'CREATE TABLE IF NOT EXISTS replication_tombstone (id BIGINT AUTO_INCREMENT PRIMARY KEY, table_name VARCHAR(64) NOT NULL, '
            . 'row_pk VARCHAR(191) NOT NULL, image LONGTEXT NOT NULL, UNIQUE KEY uniq_replication_tombstone (table_name, row_pk))'
        );
        $this->db->execute(
            'CREATE TABLE IF NOT EXISTS replication_conflict (id BIGINT AUTO_INCREMENT PRIMARY KEY, conflict_key CHAR(40) NOT NULL, '
            . 'table_name VARCHAR(64) NOT NULL, row_pk VARCHAR(191) NOT NULL, columns TEXT NOT NULL, incoming LONGTEXT NOT NULL, '
            . 'local LONGTEXT NULL, origin_node VARCHAR(64) NOT NULL, reason VARCHAR(500) NOT NULL, detected_at DATETIME NOT NULL, '
            . 'payload LONGTEXT NULL, resolved_at DATETIME NULL, '
            . 'UNIQUE KEY uniq_replication_conflict (conflict_key), KEY idx_replication_conflict_row (table_name, row_pk))'
        );
        // A database created before the journal kept the change: add what it lacks.
        foreach (['payload' => 'LONGTEXT NULL', 'resolved_at' => 'DATETIME NULL'] as $column => $type) {
            $has = $this->db->execute(
                'SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t AND COLUMN_NAME = :c',
                ['t' => 'replication_conflict', 'c' => $column],
            )->rows !== [];
            if (!$has) {
                $this->db->execute("ALTER TABLE replication_conflict ADD COLUMN {$column} {$type}");
            }
        }
        $this->wipeReplicationState();
    }

    private function wipeReplicationState(): void
    {
        $this->db->execute('DELETE FROM ' . self::ARTICLES);
        $this->db->execute('DELETE FROM replication_field_clock WHERE table_name = :t', ['t' => self::ARTICLES]);
        $this->db->execute('DELETE FROM replication_tombstone WHERE table_name = :t', ['t' => self::ARTICLES]);
        $this->db->execute('DELETE FROM replication_outbox');
        $this->db->execute('DELETE FROM replication_outbox_dead');
        $this->db->execute('DELETE FROM replication_conflict');
    }

    private function dropReplicationFixture(): void
    {
        if (isset($this->db)) {
            $this->wipeReplicationState();
            $this->db->execute('DROP TABLE IF EXISTS ' . self::ARTICLES);
            $this->orm->shutdown();
        }
    }
}
