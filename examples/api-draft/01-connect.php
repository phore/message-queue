<?php

declare(strict_types=1);

namespace Examples\MessageQueue\Connections;

require_once __DIR__ . '/connection.php';

use function Examples\MessageQueue\demoConnection;
use Phore\MessageQueue\ConnectionFactory;
use Phore\MessageQueue\ConnectionOptions;
use Phore\MessageQueue\Connector\RabbitMQ\RabbitMQConnector;
use Phore\MessageQueue\PhoreMQ;
use Phore\MessageQueue\Security\HmacSecurity;

/**
 * API-ENTWURF: Die importierten MQ-Klassen existieren noch nicht.
 * Der Docker-Broker und setup.php sind unabhängig davon verwendbar: docs/setup.md.
 * RabbitMQ ist der einzige Adapter. Keine Treiberregistrierung oder Brokerwahl.
 */

// 1. Gemeinsame Konfiguration: DSN, autoCreate und explizit unsignierter Demo-Modus.
function connectDemo(): PhoreMQ
{
    return new PhoreMQ(...demoConnection());
}

// 2. Explizite Verbindung: diese Funktion verbindet bereits beim Aufruf.
// Zugangsdaten und Signierschlüssel liefert die Anwendung, keine implizite Env-Suche.
function connectConfigured(string $username, string $password, string $sharedSecret): PhoreMQ
{
    $dsn = 'amqps://' . rawurlencode($username) . ':' . rawurlencode($password)
        . '@mq.example.org:5671/app'; // /app = namespace; Sonderzeichen percent-encoden.

    return new PhoreMQ($dsn, new ConnectionOptions(
        autoCreate: false, // Fachliche Topologie vorher einrichten.
        managementUrl: 'https://mq.example.org:15671', // Explizite Topologieprüfung.
        maxInFlight: 1, // Höchstens eine offene Zustellung pro Consumer, keine Parallelitätszahl.
        security: new HmacSecurity(
            sharedSecret: $sharedSecret,
            keyId: 'application-1',
            audience: 'user-services',
        ),
    ));
}

// 3. Direkte Adapter-Injektion hält die Interface-Grenze prüfbar.
function connectDirectly(): PhoreMQ
{
    [$dsn, $options] = demoConnection();
    return new PhoreMQ(new RabbitMQConnector($dsn), $options);
}

// 4. Optionale Factory ist nur ein Konstruktor-Helfer für dasselbe PhoreMQ.
function connectByFactory(): PhoreMQ
{
    return (new ConnectionFactory())->connect(...demoConnection());
}

// Aufrufer besitzt die hier zurückgegebene Verbindung und ruft close() in finally.
// Pro Prozess einmal erzeugen und weiterreichen. Kein Singleton.
// Konstruktor/Factory verbinden sofort; Fehler sind Exceptions, kein Lazy-Connect.
// Der Adapter gehört genau einem PhoreMQ; close() schließt seine Ressourcen.
// DTO-/Handler-Attribute bleiben erhalten; Verbindungsattribute entfallen.
// In der isolierten Demo ist unsigned ausdrücklich konfiguriert; kein automatisch
// erzeugtes HMAC-Secret. Produktion nutzt eigene Credentials, TLS und Security-Policy.
// Eine erfolgreiche Verbindung bestätigt noch keine Bereitschaft fremder Handler.
