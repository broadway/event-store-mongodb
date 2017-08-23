<?php

namespace Broadway\EventStore\MongoDB;

use MongoDB\Client;
use Broadway\Serializer\SimpleInterfaceSerializer;
use Broadway\EventStore\Management\EventStoreManagementTest;

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
    protected function tearDown()
    {
        $this->client->selectDatabase(self::$databaseName)->drop();
    }
}
