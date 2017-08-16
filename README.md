Installation:
-------------
Install package via composer 
```
$ composer require broadway/event-store-mongodb
```

> Note: to use MongoDB on PHP7 you will also need to install alcaeus/mongo-php-adapter

```
$ composer require alcaeus/mongo-php-adapter
```

Configuration
-------------

Register the services 
```yaml
parameters:
    mongodb_host: localhost
    mongodb_port: 27107
    mongodb_database: default 
    
    
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
