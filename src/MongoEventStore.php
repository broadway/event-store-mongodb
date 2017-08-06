<?php

namespace Broadway\EventStore\Mongo;

use MongoDB\Client;
use MongoCollection;
use MongoDB\Collection;
use Broadway\Domain\DateTime;
use MongoDB\Model\BSONDocument;
use Broadway\Domain\DomainMessage;
use Broadway\Serializer\Serializer;
use Broadway\EventStore\EventStore;
use Broadway\EventStore\EventVisitor;
use Broadway\Domain\DomainEventStream;
use Broadway\EventStore\Management\Criteria;
use MongoDB\Driver\Exception\BulkWriteException;
use Broadway\EventStore\EventStreamNotFoundException;
use Broadway\EventStore\Management\EventStoreManagement;
use Broadway\EventStore\Exception\DuplicatePlayheadException;
use Broadway\EventStore\Management\CriteriaNotSupportedException;

/**
 * @author Robin van der Vleuten <robin@webstronauts.co>
 */
class MongoEventStore implements EventStore, EventStoreManagement
{

    /**
     * @var Client
     */
    private $client;

    /**
     * @var Serializer
     */
    private $payloadSerializer;

    /**
     * @var Serializer
     */
    private $metadataSerializer;


    /**
     * @var Collection
     */
    private $eventCollection;
    /**
     * @var string
     */
    private $databaseName;
    /**
     * @var string
     */
    private $eventCollectionName;


    public function __construct(
        Client $client,
        Serializer $payloadSerializer,
        Serializer $metadataSerializer,
        string $databaseName,
        string $eventCollectionName
    ) {
        $this->client = $client;
        $this->payloadSerializer = $payloadSerializer;
        $this->metadataSerializer = $metadataSerializer;
        $this->databaseName = $databaseName;
        $this->eventCollectionName = $eventCollectionName;

        $this->eventCollection = $client->selectCollection($databaseName, $eventCollectionName);

    }
    
    /**
     * {@inheritdoc}
     */
    public function load($id)
    {

        $cursor = $this->eventCollection
            ->find([
                'uuid' => (string) $id,
            ],['sort' => ['payhead' => MongoCollection::ASCENDING]]);

        $events = [];

        foreach ($cursor as $event) {
            $events[] = $this->deserializeEvent($event);
        }

        if (!$events) {
            throw new EventStreamNotFoundException(sprintf('EventStream not found for aggregate with id %s', (string) $id));
        }

        return new DomainEventStream($events);
    }

    /**
     * {@inheritdoc}
     */
    public function loadFromPlayhead($id, $playhead)
    {
        $cursor = $this->eventCollection
            ->find([
                'uuid' => (string) $id,
                'playhead' => ['$gte' => $playhead]
            ],['sort' => ['payhead' => MongoCollection::ASCENDING]]);

        $events = [];
        foreach ($cursor as $event) {
            $events[] = $this->deserializeEvent($event);
        }

        return new DomainEventStream($events);
    }

    /**
     * @param BSONDocument $event
     *
     * @return DomainMessage
     */
    private function deserializeEvent(BSONDocument $event)
    {
        return new DomainMessage(
            $event['uuid'],
            $event['playhead'],
            $this->metadataSerializer->deserialize(json_decode($event['metadata'], true)),
            $this->payloadSerializer->deserialize(json_decode($event['payload'], true)),
            DateTime::fromString($event['recorded_on'])
        );
    }


    /**
     * {@inheritdoc}
     */
    public function append($id, DomainEventStream $eventStream)
    {
        $messages = [];

        foreach ($eventStream as $message) {
            $messages[] = $this->normalizeDomainMessage($message);
        }

        try {
            $this->eventCollection->insertMany($messages);
        } catch (BulkWriteException $exception) {
            throw new DuplicatePlayheadException($eventStream, $exception);
        }

    }

    /**
     * @param DomainMessage $message
     *
     * @return array
     */
    private function normalizeDomainMessage(DomainMessage $message)
    {
        return [
            'uuid'        => (string) $message->getId(),
            'playhead'    => $message->getPlayhead(),
            'metadata'    => json_encode($this->metadataSerializer->serialize($message->getMetadata())),
            'payload'     => json_encode($this->payloadSerializer->serialize($message->getPayload())),
            'recorded_on' => $message->getRecordedOn()->toString(),
            'type'        => $message->getType(),
        ];
    }


    /**
     * {@inheritdoc}
     */
    public function visitEvents(Criteria $criteria, EventVisitor $eventVisitor)
    {
        if ($criteria->getAggregateRootTypes()) {
            throw new CriteriaNotSupportedException(
                'Mongo implementation cannot support criteria based on aggregate root types.'
            );
        }

        $findCriteria = $this->getWheresForCriteria($criteria);

        return $this->eventCollection->find($findCriteria);

    }

    /**
     * @param Criteria $criteria
     * @return array
     */
    private function getWheresForCriteria(Criteria $criteria)
    {
        $wheres = [];

        if (!empty($criteria->getAggregateRootIds())) {
            $wheres['uuid'] = $criteria->getAggregateRootIds();
        }

        if (!empty($criteria->getEventTypes())) {
            $wheres['type'] = $criteria->getAggregateRootTypes();
        }

        return $wheres;
    }
    
    public function configureCollection()
    {
        $this->eventCollection->createIndex(['playhead' => 1],['unique' => true]);
    }
}
