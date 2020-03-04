<?php

/*
 * This file is part of the broadway/event-store-dbal package.
 *
 * (c) 2020 Broadway project
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Broadway\EventStore\MongoDB;

use Broadway\EventStore\Management\Testing\EventStoreManagementTest;
use Broadway\Serializer\SimpleInterfaceSerializer;
use MongoDB\Client;

class MongoDBEventStoreManagementTest extends EventStoreManagementTest
{
    protected static $databaseName = 'mongodb_test';
    protected static $eventCollectionName = 'test_events';

    /* @var Client $client */
    protected $client;

    /**
     * {@inheritdoc}
     */
    public function setUp(): void
    {
        $this->client = new Client('mongodb://localhost:27017');

        parent::setUp();
    }

    /**
     * {@inheritdoc}
     */
    protected function createEventStore()
    {
        $collection = $this->client->selectCollection(self::$databaseName, self::$eventCollectionName);

        $eventStore = new MongoDBEventStore(
            $collection,
            new SimpleInterfaceSerializer(),
            new SimpleInterfaceSerializer()
        );

        $eventStore->configureCollection();

        return $eventStore;
    }

    /**
     * {@inheritdoc}
     */
    protected function tearDown(): void
    {
        $this->client->selectDatabase(self::$databaseName)->drop();
    }
}
