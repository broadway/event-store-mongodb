<?php

namespace Broadway\EventStore\MongoDB;

use Broadway\EventStore\Management\Testing\EventStoreManagementTest;
use MongoDB\Client;
use Broadway\Serializer\SimpleInterfaceSerializer;

class MongoDBEventStoreManagementTest extends EventStoreManagementTest
{

    protected static $databaseName = 'mongodb_test';
    protected static $eventCollectionName = 'test_events';

    /* @var Client $client */
    protected $client;


    /**
     * @inheritdoc
     */
    public function setUp(): void
    {
        $this->client = new Client('mongodb://localhost:27017');

        parent::setUp();
    }

    /**
     * @inheritdoc
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
     * @inheritdoc
     */
    protected function tearDown(): void
    {
        $this->client->selectDatabase(self::$databaseName)->drop();
    }
}
