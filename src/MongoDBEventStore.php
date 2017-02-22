<?php

namespace Broadway\EventStore;

use App\MongoDB\Client;
use Broadway\Domain\DateTime;
use Broadway\Domain\DomainEventStream;
use Broadway\Domain\DomainEventStreamInterface;
use Broadway\Domain\DomainMessage;
use Broadway\EventStore\EventStoreInterface;
use Broadway\EventStore\EventStreamNotFoundException;
use Broadway\Serializer\SerializerInterface;
use MongoDB\Collection;
use MongoDB\Model\BSONDocument;

/**
 * @author Robin van der Vleuten <robin@webstronauts.co>
 */
class MongoDBEventStore implements EventStoreInterface
{
    /**
     * @var SerializerInterface
     */
    private $payloadSerializer;

    /**
     * @var SerializerInterface
     */
    private $metadataSerializer;

    /**
     * @var Collection
     */
    private $collection;

    /**
     * Constructor.
     *
     * @param Client              $mongodb
     * @param SerializerInterface $payloadSerializer
     * @param SerializerInterface $metadataSerializer
     * @param string              $collectionName
     */
    public function __construct(Client $mongodb, SerializerInterface $payloadSerializer, SerializerInterface $metadataSerializer, $collectionName)
    {
        $this->payloadSerializer = $payloadSerializer;
        $this->metadataSerializer = $metadataSerializer;

        $this->collection = $mongodb->selectCollectionFromDefaultDatabase($collectionName);
    }

    /**
     * {@inheritdoc}
     */
    public function load($id)
    {
        $cursor = $this->collection->find(['uuid' => (string) $id]);

        $messages = [];
        foreach ($cursor as $message) {
            $messages[] = $this->denormalizeDomainMessage($message);
        }

        if (empty($messages)) {
            throw new EventStreamNotFoundException(sprintf('EventStream not found for aggregate with id %s', (string) $id));
        }

        return new DomainEventStream($messages);
    }

    /**
     * {@inheritdoc}
     */
    public function append($id, DomainEventStreamInterface $eventStream)
    {
        $messages = [];

        foreach ($eventStream as $message) {
            $messages[] = $this->normalizeDomainMessage($message);
        }

        $this->collection->insertMany($messages);
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
     * @param BSONDocument $message
     *
     * @return DomainMessage
     */
    private function denormalizeDomainMessage(BSONDocument $message)
    {
        return new DomainMessage(
            $message['uuid'],
            $message['playhead'],
            $this->metadataSerializer->deserialize(json_decode($message['metadata'], true)),
            $this->payloadSerializer->deserialize(json_decode($message['payload'], true)),
            DateTime::fromString($message['recorded_on'])
        );
    }
}
