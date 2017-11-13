<?php

namespace Broadway\EventStore\MongoDB;

use Broadway\EventStore\Testing\EventStoreTest;
use MongoDB\Client;
use Broadway\Serializer\SimpleInterfaceSerializer;

class MongoDBEventStoreTest extends EventStoreTest
{
    protected static $databaseName = 'mongodb_test';
    protected static $eventCollectionName = 'test_events';

    /* @var Client $client */
    protected $client;

    /**
     * @inheritdoc
     */
    public function setUp()
    {
        $this->client = new Client('mongodb://localhost:27017');
        $this->client->selectDatabase(self::$databaseName)->drop();

        $collection = $this->client->selectCollection(self::$databaseName, self::$eventCollectionName);

        $this->eventStore = new MongoDBEventStore(
            $collection,
            new SimpleInterfaceSerializer(),
            new SimpleInterfaceSerializer()
        );

        $this->eventStore->configureCollection();
    }

    /**
     * @inheritdoc
     */
    public function tearDown()
    {
        $this->client->selectDatabase(self::$databaseName)->drop();
    }
}
