<?php

declare(strict_types=1);

namespace Examples\MessageQueue\Connections;

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
// Die Beispiele 02–09 mit Factory bleiben gültig: Sie erhalten dasselbe PhoreMQ.
// Gegen MessageQueueInterface typisierte Anwendungskomponenten bleiben möglich.
// Kein Singleton; der Connector gehört exklusiv zu dieser MQ-Instanz.
// Konstruktor/Factory verbinden sofort; Fehler werfen die Exceptions aus § 11.
// Jede verwendete MQ-Instanz anschließend in finally mit $mq->close() freigeben.
