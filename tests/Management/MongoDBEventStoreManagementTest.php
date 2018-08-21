<?php

namespace Broadway\EventStore\MongoDB\Tests\Management;

use Broadway\EventStore\Management\Testing\EventStoreManagementTest;
use Broadway\EventStore\MongoDB\MongoDBEventStore;
use Broadway\Serializer\SimpleInterfaceSerializer;
use MongoDB\Client;

class MongoDBEventStoreManagementTest extends EventStoreManagementTest
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
        $this->client = new Client('mongodb://mongo:27017');

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
    protected function tearDown()
    {
        $this->client->selectDatabase(self::$databaseName)->drop();
    }
}
