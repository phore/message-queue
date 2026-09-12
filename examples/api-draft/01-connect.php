<?php

declare(strict_types=1);

namespace Examples\MessageQueue\Connections;

use Phore\MessageQueue\Attribute\QueueConnection;
use Phore\MessageQueue\ConnectionFactory;
use Phore\MessageQueue\ConnectionOptions;
use Phore\MessageQueue\Connector\Redis\RedisConnection;
use Phore\MessageQueue\Connector\Redis\RedisStreamsConnector;
use Phore\MessageQueue\MessageQueueInterface;
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

// 1. URL mit Benutzername und Passwort; Sonderzeichen percent-encoden.
function connectByUrl(string $username, string $password, string $sharedSecret): MessageQueueInterface
{
    $dsn = 'redis://' . rawurlencode($username) . ':' . rawurlencode($password)
        . '@127.0.0.1:6379/0?prefix=demo';

    return (new ConnectionFactory())->connect($dsn, optionsForDevelopment($sharedSecret));
}

// 2. Direkter Konnektor: dieselbe gemeinsame API und Sicherheitskette.
function connectDirectly(string $username, string $password, string $sharedSecret): MessageQueueInterface
{
    $connector = new RedisStreamsConnector(new RedisConnection(
        host: '127.0.0.1',
        port: 6379,
        database: 0,
        username: $username,
        password: $password,
        prefix: 'demo',
    ));

    return (new ConnectionFactory())->fromConnector($connector, optionsForDevelopment($sharedSecret));
}

// 3. Attribut-Konfiguration einer lokalen Verbindung; keine Secrets im Attribut.
#[QueueConnection(dsn: 'redis://127.0.0.1:6379/0?prefix=demo')]
final class LocalConnection
{
}

function connectWithAttribute(string $sharedSecret): MessageQueueInterface
{
    return (new ConnectionFactory())->fromAttributes(
        LocalConnection::class,
        optionsForDevelopment($sharedSecret),
    );
}

// 4. Austauschbarer Security-Provider: z. B. später eigener PGP-Provider.
// Der Provider muss Topic/Audience, Keyring und Zeitprüfung implementieren.
function connectWithSecurityProvider(string $dsn, MessageSecurityInterface $security): MessageQueueInterface
{
    return (new ConnectionFactory())->connect($dsn, new ConnectionOptions(
        security: $security,
    ));
}

// Jede verwendete Connection anschließend mit $mq->close() freigeben.
