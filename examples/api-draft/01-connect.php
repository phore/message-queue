<?php

declare(strict_types=1);

namespace Examples\MessageQueue\Connections;

use Phore\MessageQueue\SubscriptionOptions;
use Phore\MessageQueue\Security\UnsignedSecurity;
use Phore\MessageQueue\RunOptions;
use Phore\MessageQueue\MessageContext;
use Phore\MessageQueue\Durability;
use Phore\MessageQueue\Development\UnixDevBroker;
use Phore\MessageQueue\Connector\InMemory\InMemoryConnector;
use Phore\MessageQueue\Connector\InMemory\InMemoryBroker;
use Phore\MessageQueue\Attribute\QueueConnection;
use Phore\MessageQueue\ConnectionFactory;
use Phore\MessageQueue\ConnectionOptions;
use Phore\MessageQueue\Connector\Redis\RedisConnection;
use Phore\MessageQueue\Connector\Redis\RedisStreamsConnector;
use Phore\MessageQueue\PhoreMQ;
use Phore\MessageQueue\ConnectorInterface;
use Phore\MessageQueue\Schema\PhoreSchemaMapper;
use Phore\MessageQueue\Security\HmacSecurity;
use Phore\MessageQueue\Security\MessageSecurityInterface;

/**
 * API-ENTWURF: Die importierten MQ-Klassen existieren noch nicht.
 * Siehe ../../docs/proposals/2026-09-12-message-queue-api.md, §§ 3–4, 8.
 * Nach Implementierung lädt die Anwendung ihren Composer-Autoloader und ruft
 * genau eine Verbindungsvariante auf. Secrets kommen vom Aufrufer.
 */

function optionsForDevelopment(string $sharedSecret): ConnectionOptions
{
    return new ConnectionOptions(
        security: new HmacSecurity(
            sharedSecret: $sharedSecret,
            keyId: 'development-1',
            audience: 'user-services-development',
        ),
        schemaMapper: new PhoreSchemaMapper(), // Optional; weglassen für reine Arrays.
        autoCreate: true, // Nur Entwicklung: Topics/Subscriptions vor Publish anlegen.
    );
}

// 1. Normalfall: new PhoreMQ mit DSN und Optionen. Sonderzeichen percent-encoden.
function connectByUrl(string $username, string $password, string $sharedSecret): PhoreMQ
{
    $dsn = 'redis://' . rawurlencode($username) . ':' . rawurlencode($password)
        . '@127.0.0.1:6379/0?prefix=demo';

    return new PhoreMQ(connection: $dsn, options: optionsForDevelopment($sharedSecret));
}

// 2. Direkter Konnektor: dieselbe gemeinsame API und Sicherheitskette.
function connectDirectly(string $username, string $password, string $sharedSecret): PhoreMQ
{
    $connector = new RedisStreamsConnector(new RedisConnection(
        host: '127.0.0.1',
        port: 6379,
        database: 0,
        username: $username,
        password: $password,
        prefix: 'demo',
    ));

    return new PhoreMQ($connector, optionsForDevelopment($sharedSecret));
}

// 3. Factory als gleichwertige Alternative: Rückgabe ist ebenfalls PhoreMQ.
function connectByFactory(string $dsn, string $sharedSecret): PhoreMQ
{
    return (new ConnectionFactory())->connect($dsn, optionsForDevelopment($sharedSecret));
}

function connectConnectorByFactory(ConnectorInterface $connector, string $sharedSecret): PhoreMQ
{
    return (new ConnectionFactory())->fromConnector($connector, optionsForDevelopment($sharedSecret));
}

// 4. Attribut-Konfiguration einer lokalen Verbindung; keine Secrets im Attribut.
#[QueueConnection(dsn: 'redis://127.0.0.1:6379/0?prefix=demo')]
final class LocalConnection
{
}

function connectWithAttribute(string $sharedSecret): PhoreMQ
{
    return (new ConnectionFactory())->fromAttributes(
        LocalConnection::class,
        optionsForDevelopment($sharedSecret),
    );
}

// 5. Austauschbarer Security-Provider: z. B. später eigener PGP-Provider.
// Der Provider muss Topic/Audience, Keyring und Zeitprüfung implementieren.
function connectWithSecurityProvider(string $dsn, MessageSecurityInterface $security): PhoreMQ
{
    return new PhoreMQ($dsn, new ConnectionOptions(
        security: $security,
    ));
}

// Eine dieser Varianten EINMAL im Bootstrap aufrufen und das erhaltene Objekt
// für publish/subscribe/request/respond/check/run weiterreichen (z. B. per DI).
// Die Beispiele 02–10 verwenden den einfachen Konstruktor; die Factory liefert denselben Typ.
// Gegen MessageQueueInterface typisierte Anwendungskomponenten bleiben möglich.
// Kein Singleton; der Connector gehört exklusiv zu dieser MQ-Instanz.
// Konstruktor/Factory verbinden sofort; Fehler werfen die Exceptions aus § 11.
// Jede verwendete MQ-Instanz anschließend in finally mit $mq->close() freigeben.

// 6. Einfacher lokaler Einstieg für Beispiele 02–10 (geplanter file-Adapter).
// Keine Netzwerkdienste oder zusätzlichen ConnectionOptions nötig.
// SQLite-basierte Queue für Prozesse desselben lokalen Benutzers; ext-pdo_sqlite nötig.
// Das Verzeichnis enthält Queue, Fehlerablage, Attachments und einen persistenten
// lokalen HMAC-Key. Private Rechte werden geprüft; unsichere bestehende Pfade abgelehnt.
// Unique Reply-Endpunkte pro Instanz, Auto-Provisionierung nur innerhalb dieses Roots.
// Schema-Bridge wird genutzt, wenn phore/schema installiert ist; DTO-Nutzung ohne
// verfügbare Bridge scheitert weiterhin ausdrücklich. Keine Installation im Konstruktor.
// Alle zusammengehörigen Prozesse verwenden denselben Root; unabhängige Demos einen
// frischen Root wählen. Kein NFS-/Multi-Host-/Produktionsbroker; Redis bleibt Standard.
function connectLocal(): PhoreMQ
{
    return new PhoreMQ('file:///tmp/phore-mq-demo');
}

// Die folgenden detaillierten lokalen Verbindungsvarianten sind hier zentralisiert.
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
        // Höchstens 1 Zustellversuch(e) insgesamt oder 1 s Gesamtbudget; erstes Limit gewinnt.
        // Normale Rückkehr, keine Mindestzahl/Timeout-Exception; Details in 02-programmatic.php.
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
        // Höchstens 1 Zustellversuch(e) insgesamt oder 5 s Gesamtbudget; erstes Limit gewinnt.
        // Normale Rückkehr, keine Mindestzahl/Timeout-Exception; Details in 02-programmatic.php.
        $mq->run(new RunOptions(maxMessages: 1, maxSeconds: 5));
    } finally {
        $mq->close();
    }
}

// Alternative mit echter Redis-Semantik, ohne eigenen Dev-Broker:
// $factory->connect('redis+unix:///run/redis/redis.sock?db=0', $options);
