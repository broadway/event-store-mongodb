<?php

namespace Broadway\EventStore\Mongo;

use MongoDB\Client;
use MongoDB\Database;
use Broadway\EventStore\EventStoreTest;
use Broadway\Serializer\SimpleInterfaceSerializer;

class MongoEventStoreTest extends EventStoreTest
{

    protected static $databaseName = 'mongodb_test';
    protected static $eventCollectionName = 'test_events';


    /* @var $database Database */
    protected $database;


    /**
     * @inheritdoc
     */
    public function setUp()
    {
        $client = new Client('mongodb://localhost:27017');
        $this->database = $client->selectDatabase(self::$databaseName);
        $this->database->drop();

        $this->eventStore = new MongoEventStore(
            $client,
            new SimpleInterfaceSerializer(),
            new SimpleInterfaceSerializer(),
            self::$databaseName,
            self::$eventCollectionName
        );

        $this->eventStore->configureCollection();
    }

    /**
     * @inheritdoc
     */
    public function tearDown()
    {
        $this->database->drop();
    }
}
