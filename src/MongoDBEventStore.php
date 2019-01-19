<?php

namespace Broadway\EventStore\MongoDB;

use MongoDB\Collection;
use MongoDB\Model\BSONDocument;
use MongoDB\Driver\Exception\BulkWriteException;
use Broadway\Domain\DateTime;
use Broadway\Domain\DomainMessage;
use Broadway\Serializer\Serializer;
use Broadway\Domain\DomainEventStream;
use Broadway\EventStore\EventStore;
use Broadway\EventStore\EventVisitor;
use Broadway\EventStore\Management\Criteria;
use Broadway\EventStore\EventStreamNotFoundException;
use Broadway\EventStore\Management\EventStoreManagement;
use Broadway\EventStore\Exception\DuplicatePlayheadException;
use Broadway\EventStore\Management\CriteriaNotSupportedException;

/**
 * @author Robin van der Vleuten <robin@webstronauts.co>
 */
class MongoDBEventStore implements EventStore, EventStoreManagement
{

    /**
     * @var Collection
     */
    private $eventCollection;

    /**
     * @var Serializer
     */
    private $payloadSerializer;

    /**
     * @var Serializer
     */
    private $metadataSerializer;


    public function __construct(
        Collection $eventCollection,
        Serializer $payloadSerializer,
        Serializer $metadataSerializer
    ) {
        $this->eventCollection = $eventCollection;
        $this->payloadSerializer = $payloadSerializer;
        $this->metadataSerializer = $metadataSerializer;
    }

    /**
     * {@inheritdoc}
     */
    public function load($id): DomainEventStream
    {
        $cursor = $this->eventCollection
            ->find([
                'uuid' => (string) $id,
            ],['sort' => ['playhead' => 1]]);

        $domainMessages = [];

        foreach ($cursor as $domainMessage) {
            $domainMessages[] = $this->denormalizeDomainMessage($domainMessage);
        }

        if (empty($domainMessages)) {
            throw new EventStreamNotFoundException(sprintf('EventStream not found for aggregate with id %s', (string) $id));
        }

        return new DomainEventStream($domainMessages);
    }

    /**
     * {@inheritdoc}
     */
    public function loadFromPlayhead($id, int $playhead): DomainEventStream
    {
        $cursor = $this->eventCollection
            ->find([
                'uuid' => (string) $id,
                'playhead' => ['$gte' => $playhead]
            ],['sort' => ['playhead' => 1]]);

        $domainMessages = [];

        foreach ($cursor as $domainMessage) {
            $domainMessages[] = $this->denormalizeDomainMessage($domainMessage);
        }

        return new DomainEventStream($domainMessages);
    }

    /**
     * @param BSONDocument $event
     *
     * @return DomainMessage
     */
    private function denormalizeDomainMessage(BSONDocument $event)
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
    public function append($id, DomainEventStream $eventStream): void
    {
        // noop to ensure that an error will be thrown early if the ID
        // is not something that can be converted to a string. If we
        // let this move on without doing this MongoDB will eventually
        // give us a hard time but the true reason for the problem
        // will be obfuscated.
        $id = (string) $id;

        $messages = [];

        foreach ($eventStream as $message) {
            $messages[] = $this->normalizeDomainMessage($message);
        }

        try {
            $this->eventCollection->insertMany($messages);
        } catch (BulkWriteException $bulkWriteException) {
            foreach ($bulkWriteException->getWriteResult()->getWriteErrors() as $writeError){
                if($writeError->getCode() === 11000) throw new DuplicatePlayheadException($eventStream, $bulkWriteException);
            }
            throw $bulkWriteException;
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
    public function visitEvents(Criteria $criteria, EventVisitor $eventVisitor): void
    {
        if ($criteria->getAggregateRootTypes()) {
            throw new CriteriaNotSupportedException(
                'Mongo implementation cannot support criteria based on aggregate root types.'
            );
        }

        $findBy = $this->buildFindByCriteria($criteria);

        foreach($this->eventCollection->find($findBy) as $event){
            $eventVisitor->doWithEvent($this->denormalizeDomainMessage($event));
        }
    }

    /**
     * @param Criteria $criteria
     * @return array
     */
    private function buildFindByCriteria(Criteria $criteria)
    {
        $findBy = [];
        if ($criteria->getAggregateRootIds()) {
            $findBy['uuid'] = ['$in' => $criteria->getAggregateRootIds()];
        }

        if ($criteria->getEventTypes()) {
            $findBy['type'] = ['$in' => $criteria->getEventTypes()];
        }
        return $findBy;
    }

    public function configureCollection()
    {
        $this->eventCollection->createIndex(['uuid' => 1, 'playhead' => 1],['unique' => true]);
    }
}
