# broadway/event-store-mongodb

![build status](https://github.com/broadway/event-store-mongodb/actions/workflows/ci.yml/badge.svg)

Installation:
-------------
Install package via composer 
```
$ composer require broadway/event-store-mongodb
```

Configuration
-------------

Register the services 
```yaml
parameters:
    mongodb_host: localhost
    mongodb_port: 27107
    mongodb_database: default 
    
services:
    broadway.event_store.mongodb_client:
        class: MongoDB\Client
        arguments: ['mongodb://%mongodb_host%:%mongodb_port%']
    
    broadway.event_store.mongodb_collection:
        class: MongoDB\Collection
        factory: ['@broadway.event_store.mongodb_client', selectCollection]
        arguments: ['%mongodb_database%', 'events']
    
    broadway.event_store.mongodb:
        class: Broadway\EventStore\MongoDB\MongoDBEventStore
        arguments: ['@broadway.event_store.mongodb_collection', '@broadway.serializer.payload', '@broadway.serializer.metadata']

```

Create index for events collection 
```javascript
db.getCollection('events').createIndex({'uuid': 1, 'playhead': 1},{'unique': 1});
```

Testing
-------
For testing you need a running MongoDB instance.
To start a local MongoDB you can use the provided [docker-compose.yml](https://docs.docker.com/compose/compose-file/):

```
docker-compose up -d
```

To run the tests:

```
./vendor/bin/phpunit
```
