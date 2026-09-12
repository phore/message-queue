<?php

declare(strict_types=1);

namespace Examples\MessageQueue\FilesAndLocal;

use Phore\MessageQueue\Attachment;
use Phore\MessageQueue\ConnectionFactory;
use Phore\MessageQueue\ConnectionOptions;
use Phore\MessageQueue\Connector\InMemory\InMemoryBroker;
use Phore\MessageQueue\Connector\InMemory\InMemoryConnector;
use Phore\MessageQueue\Development\UnixDevBroker;
use Phore\MessageQueue\Durability;
use Phore\MessageQueue\MessageContext;
use Phore\MessageQueue\Payload\LocalPayloadStore;
use Phore\MessageQueue\PublishOptions;
use Phore\MessageQueue\RunOptions;
use Phore\MessageQueue\Security\HmacSecurity;
use Phore\MessageQueue\Security\UnsignedSecurity;
use Phore\MessageQueue\SubscriptionOptions;

/**
 * API-ENTWURF, noch nicht ausführbar. Proposal §§ 8–10.
 * Dateispeicher und UnixDevBroker sind ausdrücklich Anschlussphase.
 * Pfade und Secrets werden durch den Aufrufer festgelegt, keine Environment-Reads.
 */

function zipDemo(string $dsn, string $sharedSecret, string $storeDirectory, string $zipPath, string $outputPath): void
{
    $mq = (new ConnectionFactory())->connect($dsn, new ConnectionOptions(
        security: new HmacSecurity(
            sharedSecret: $sharedSecret,
            keyId: 'development-1',
            audience: 'export-services-development',
        ),
        payloadStore: new LocalPayloadStore(directory: $storeDirectory),
        autoCreate: true,
    ));

    try {
        $mq->subscribe('exports', 'archive-importer', function (array $data, MessageContext $context) use ($outputPath): void {
            // Verifiziert Größe und Digest vor Freigabe des Ziels; kein Entpacken.
            // Ziel kommt aus lokaler Konfiguration, niemals aus dem Dateinamen.
            $context->attachment('archive')->copyTo($outputPath);
            printf("Export %s liegt verifiziert bereit.\n", $data['exportId']);
        }, new SubscriptionOptions(type: 'export.ready.v1'));

        $mq->publish('exports', 'export.ready.v1', ['exportId' => 'export-42'], new PublishOptions(
            attachments: [
                'archive' => Attachment::fromPath($zipPath, contentType: 'application/zip'),
            ],
        ));
        $mq->run(new RunOptions(maxMessages: 1, maxSeconds: 30));
    } finally {
        $mq->close();
    }
}

// In einem Test teilen zwei Connections denselben prozessinternen Broker.
// Auch dieser Konnektor nutzt Codec/Envelope statt PHP-Objektreferenzen.
function inMemoryDemo(): void
{
    $broker = new InMemoryBroker();
    $factory = new ConnectionFactory();
    $options = new ConnectionOptions(security: new UnsignedSecurity(), autoCreate: true);
    $sender = $factory->fromConnector(new InMemoryConnector($broker), $options);
    $receiver = $factory->fromConnector(new InMemoryConnector($broker), $options);

    try {
        $receiver->subscribe('users', 'local-users', function (array $data): void {
            printf("Memory: %s\n", $data['userId']);
        }, new SubscriptionOptions(durability: Durability::Volatile));
        $sender->publish('users', 'user.created.v1', ['userId' => 'local-1']);
        $receiver->run(new RunOptions(maxMessages: 1, maxSeconds: 1));
    } finally {
        $sender->close();
        $receiver->close();
    }
}

// Eigener Prozess A: Dev-Broker starten. Elternverzeichnis muss privat sein.
// Volatil: Neustart des Brokers verliert gespeicherte Nachrichten/Subscriptions.
function runUnixBroker(string $socketPath): void
{
    $broker = new UnixDevBroker(socketPath: $socketPath, socketMode: 0600);
    try {
        $broker->run();
    } finally {
        $broker->close();
    }
}

// Prozess B (Receiver) und C (Sender) können dieselbe lokale DSN verwenden.
// Der Receiver muss seine Subscription anlegen, bevor der Sender publiziert.
function unixClientDemo(string $socketDsn): void
{
    // Beispiel: unix:///run/user/1000/phore-mq.sock
    // Bewusst unsigniert nur für isolierte lokale Entwicklung.
    $mq = (new ConnectionFactory())->connect($socketDsn, new ConnectionOptions(
        security: new UnsignedSecurity(),
        autoCreate: true,
    ));
    try {
        $mq->subscribe('local', 'local-worker', function (array $data): void {
            printf("Unix: %s\n", $data['value']);
        }, new SubscriptionOptions(durability: Durability::Volatile));
        $mq->publish('local', 'ping.v1', ['value' => 'hello']);
        $mq->run(new RunOptions(maxMessages: 1, maxSeconds: 5));
    } finally {
        $mq->close();
    }
}

// Alternative mit echter Redis-Semantik, ohne eigenen Dev-Broker:
// $factory->connect('redis+unix:///run/redis/redis.sock?db=0', $options);
