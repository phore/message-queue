<?php

declare(strict_types=1);

namespace Examples\MessageQueue\Programmatic;

use Phore\MessageQueue\ConnectionFactory;
use Phore\MessageQueue\ConnectionOptions;
use Phore\MessageQueue\Exception\MessageValidationException;
use Phore\MessageQueue\MessageContext;
use Phore\MessageQueue\MessageRegistry;
use Phore\MessageQueue\Schema\PhoreSchemaMapper;
use Phore\MessageQueue\Security\HmacSecurity;
use Phore\MessageQueue\SubscriptionOptions;

/**
 * API-ENTWURF, noch nicht ausführbar. Proposal §§ 5–6 und 11.
 * Beispielaufruf nach Implementierung: demo($dsn, $sharedSecret).
 * Dafür einen frischen Redis-Prefix verwenden: zwei neue Subscriptions werden
 * vor dem Publish gebunden. Bei wiederverwendetem Prefix kann Backlog anliegen.
 */

// Beliebige eigene Klasse: kein gemeinsames SDK und keine Attribute notwendig.
final class LocalUserCreated
{
    public string $userId;
    public string $email;
}

function demo(string $dsn, string $sharedSecret): void
{
    $registry = new MessageRegistry();
    $registry->register('user.created.v1', LocalUserCreated::class, topic: 'users');

    $mq = (new ConnectionFactory())->connect($dsn, new ConnectionOptions(
        security: new HmacSecurity(
            sharedSecret: $sharedSecret,
            keyId: 'development-1',
            audience: 'user-services-development',
        ),
        registry: $registry,
        schemaMapper: new PhoreSchemaMapper(),
        autoCreate: true,
    ));

    try {
        // Array: für diesen Typ validiert der registrierte Contract die Struktur.
        $mq->subscribe('users', 'audit-users', function (array $data, MessageContext $context): void {
            printf("Audit: %s / %s\n", $context->messageId, $data['userId']);
        }, new SubscriptionOptions(type: 'user.created.v1'));

        // Eigene lokale DTO-Klasse: Reflection erkennt den ersten Parameter.
        // Sender darf andere Klasse/anderen Namespace oder ein Array verwenden.
        $mq->subscribe('users', 'billing-users', function (LocalUserCreated $user): void {
            printf("Billing: %s / %s\n", $user->userId, $user->email);
        }, new SubscriptionOptions(type: 'user.created.v1'));

        // Weiteres Topic im selben Worker, völlig ohne Schema/DTO-Zuordnung.
        $mq->subscribe('telemetry', 'audit-telemetry', function (array $data): void {
            printf("Telemetry: %s\n", json_encode($data, JSON_THROW_ON_ERROR));
        });

        // Manuelles Topic und fachlicher Typ; zusätzlicher Schlüssel ist kompatibel.
        $mq->publish('users', 'user.created.v1', [
            'userId' => 'u-123',
            'email' => 'user@example.org',
            'displayName' => 'Optionales neues Feld',
        ]);

        // Run-Optionen (Sekunden sind Laufzeiten, keine Pollingintervalle):
        // maxMessages: höchstens so viele abgeschlossene fachliche Zustellversuche
        // in DIESEM run(), über alle registrierten Subscriptions zusammen.
        // Fan-out an zwei Gruppen zählt zweimal; Redelivery zählt erneut.
        // Auch behandelte Retry-/Reject-/Validierungsfehler zählen, nicht nur Erfolge.
        // Interne Health-/RPC-Replies und leere Polls zählen nicht.
        // maxSeconds: Gesamtbudget ab run()-Start, inklusive Warten und Verarbeitung.
        // idleTimeoutSeconds: optional; beendet nach so langer zusammenhängender
        // Wartezeit ohne fachliche Zustellung. Beginnt beim Eintritt ins Warten neu;
        // Handlerlaufzeit zählt nicht als Leerlauf. Beispiel: run(idleTimeoutSeconds: 2).
        // Das zuerst erreichte Limit beendet normal: keine Timeout-Exception.
        // Laufende synchrone Handler werden nicht hart abgebrochen; maxSeconds kann
        // deshalb überschritten werden. Nach Fristablauf startet kein weiterer Handler.
        // minMessages gibt es nicht: keine garantierte Mindestzahl erzwingen.
        // Ohne gesetztes Limit gilt für diese Grenze unbegrenzt; run() läuft bis stop()
        // oder einem Infrastrukturfehler. Limits müssen positiv sein (kein 0/-1).
        // Dieselben Felder sind alternativ in RunOptions verfügbar (siehe 05-rpc.php).
        // Zwei unabhängige Subscriptions verarbeiten je dieselbe Nachricht.
        $mq->run(maxMessages: 2, maxSeconds: 10); // Höchstens 2 Versuche oder 10 s, ggf. weniger.

        // Typobjekt ohne Attribute: publish löst das programmatische Mapping auf.
        $user = new LocalUserCreated();
        $user->userId = 'u-456';
        $user->email = 'other@example.org';
        $mq->publish($user);
        $mq->run(maxMessages: 2, maxSeconds: 10); // Höchstens 2 Versuche oder 10 s, ggf. weniger.

        // Ohne Contract registrierter Typ: JSON-Daten, keine Schema-Hydration.
        $mq->publish('telemetry', 'heartbeat.v1', ['service' => 'billing']);
        $mq->run(maxMessages: 1, maxSeconds: 10); // Höchstens 1 Versuch oder 10 s.

        // Aussagekräftiger lokaler Fehler, bevor die Nachricht versendet wird.
        try {
            $mq->publish('users', 'user.created.v1', ['userId' => 'missing-email']);
        } catch (MessageValidationException $exception) {
            // Erwartet: user.created.v1: $.email: required property is missing
            printf("Ungültige Nachricht: %s\n", $exception->getMessage());
        }
    } finally {
        $mq->close();
    }
}
