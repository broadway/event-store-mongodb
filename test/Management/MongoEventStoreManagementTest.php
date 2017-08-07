<?php

namespace Broadway\EventStore\Mongo;

use Broadway\EventStore\Management\EventStoreManagementTest;
use MongoDB\Client;
use MongoDB\Database;
use Broadway\Serializer\SimpleInterfaceSerializer;

class MongoEventStoreManagementTest extends EventStoreManagementTest
{

    protected static $databaseName = 'mongodb_test';
    protected static $eventCollectionName = 'test_events';
    
    /**
     * @var Client
     */
    protected $client;


    /**
     * @inheritdoc
     */
    public function setUp()
    {
        $this->client =  new Client('mongodb://localhost:27017');
        $this->client->selectDatabase(self::$databaseName)->drop();

        parent::setUp();
    }

    /**
     * @inheritdoc
     */
    protected function createEventStore()
    {
        $eventStore = new MongoEventStore(
            $this->client,
            new SimpleInterfaceSerializer(),
            new SimpleInterfaceSerializer(),
            self::$databaseName,
            self::$eventCollectionName
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
