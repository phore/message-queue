<?php

declare(strict_types=1);

namespace Examples\MessageQueue\Attributes;

use Phore\MessageQueue\Attribute\MessageType;
use Phore\MessageQueue\Attribute\Subscribe;
use Phore\MessageQueue\PhoreMQ;
use Phore\MessageQueue\SubscriptionOptions;
use Phore\MessageQueue\Exception\MessageMappingException;
use Phore\MessageQueue\Exception\InvalidHandlerException;
use Phore\MessageQueue\ConnectionOptions;
use Phore\MessageQueue\MessageContext;
use Phore\MessageQueue\MessageQueueInterface;
use Phore\MessageQueue\RunOptions;
use Phore\MessageQueue\Schema\PhoreSchemaMapper;
use Phore\MessageQueue\Security\HmacSecurity;

/**
 * API-ENTWURF, noch nicht ausführbar. Proposal §§ 4–6.
 * Für ein echtes SDK würde T_UserCreated in einer eigenen Composer-Library
 * liegen. Hier bleiben alle Teile zum Lesen in einer Beispieldatei.
 */

// Feste Gruppe: mehrere Worker mit dieser Vorgabe TEILEN die Zustellungen.
#[MessageType('user.created.v1', topic: 'users', subscription: 'sdk-users')]
final class T_UserCreated
{
    public string $userId;
    public string $email;
}

final class UserHandlers
{
    // Alle drei Werte kommen aus T_UserCreated. Keine doppelte Definition.
    #[Subscribe]
    public function onCreated(T_UserCreated $user, MessageContext $context): void
    {
        printf("SDK: %s / %s\n", $context->messageId, $user->email);
        // Erfolgreiche Rückkehr bestätigt automatisch.
    }

    // Auch Attribute erzwingen keine Typisierung: hier unveränderte Array-Daten.
    #[Subscribe(topic: 'users', subscription: 'raw-users', type: 'user.created.v1')]
    public function onRawCreated(array $data): void
    {
        printf("Array: %s\n", json_encode($data, JSON_THROW_ON_ERROR));
    }
}

function createConnection(string $dsn, string $sharedSecret): MessageQueueInterface
{
    return new PhoreMQ($dsn, new ConnectionOptions(
        security: new HmacSecurity(
            sharedSecret: $sharedSecret,
            keyId: 'development-1',
            audience: 'user-services-development',
        ),
        schemaMapper: new PhoreSchemaMapper(),
        autoCreate: true,
    ));
}

function send(MessageQueueInterface $mq): void
{
    $user = new T_UserCreated();
    $user->userId = 'u-789';
    $user->email = 'sdk-user@example.org';

    $mq->emit($user); // Liest MessageType, validiert und serialisiert.

    // Gleichwertige explizite API, etwa für eine andere Anwendung ohne SDK:
    $mq->publish('users', 'user.created.v1', [
        'userId' => 'u-790',
        'email' => 'manual@example.org',
    ]);
}

function demo(string $dsn, string $sharedSecret): void
{
    $mq = createConnection($dsn, $sharedSecret);
    try {
        // Attributvariante: Resolver liest Methodensignatur und DTO-Metadaten.
        $mq->registerHandlers(new UserHandlers());
        send($mq);
        $mq->run(new RunOptions(maxMessages: 4, maxSeconds: 10));
    } finally {
        $mq->close();
    }
}

// Getrennte Prozesse: Empfänger legt/bindet Subscriptions vor dem ersten Senden
// an und ruft run() auf. Sender ruft danach send() auf seiner eigenen Connection
// auf. Beide verwenden denselben Broker-Prefix, HMAC-Key und dieselbe Audience.


// Alternative zur Attributregistrierung: nur den Callback übergeben.
// Auf einer eigenen MQ-Instanz statt demo()/registerHandlers() ausführen.
function demoCallback(string $dsn, string $sharedSecret): void
{
    $mq = createConnection($dsn, $sharedSecret);
    try {
        $mq->subscribe(function (T_UserCreated $user, MessageContext $context): void {
            printf("Callback: %s / %s\n", $context->messageId, $user->email);
        }); // users + sdk-users + user.created.v1; automatische Hydration.
        send($mq);
        // Empfangsschleife für registrierte Handler, kein verzögertes emit/publish.
        $mq->run(new RunOptions(maxMessages: 2, maxSeconds: 10));
    } finally {
        $mq->close();
    }
}

// Alternativ ist auch $mq->subscribe([$handlers, 'onCreated']) möglich.
// NICHT zusätzlich dieselbe Methode über registerHandlers() registrieren.

// Dieser Contract ist topic- und gruppenunabhängig; der Wire-Typ steht nur hier.
#[MessageType('audit.entry.v1')]
final class T_AuditEntry
{
    public string $text;
}

final class AuditHandlers
{
    // Nur offene Angaben ergänzen; type ergibt sich aus T_AuditEntry.
    #[Subscribe(topic: 'audit.users', subscription: 'audit-reader')]
    public function onEntry(T_AuditEntry $entry): void
    {
        printf("Audit: %s\n", $entry->text);
    }
}

function demoMultipleTopics(string $dsn, string $sharedSecret): void
{
    $mq = createConnection($dsn, $sharedSecret);
    try {
        $handler = static function (T_AuditEntry $entry): void {
            printf("Audit: %s\n", $entry->text);
        };
        foreach (['audit.users', 'audit.billing'] as $topic) {
            $mq->subscribe($handler, options: new SubscriptionOptions(
                topic: $topic, subscription: 'audit-reader',
            ));
        }
        // Alternative für audit.users: registerHandlers(new AuditHandlers()).
        // Die Alternative ersetzt dessen obige Registrierung, nicht zusätzlich aufrufen.
        $entry = new T_AuditEntry();
        $entry->text = 'Ein Vorgang wurde abgeschlossen.';
        $mq->publish('audit.users', 'audit.entry.v1', $entry);
        $mq->publish('audit.billing', 'audit.entry.v1', $entry);
        // emit($entry) wäre MAPPING_INCOMPLETE: kein festes Topic auf diesem DTO.
        $mq->run(new RunOptions(maxMessages: 2, maxSeconds: 10));
    } finally {
        $mq->close();
    }
}

// Separates Fehlerbeispiel; kein Workerstart, kein Senden erforderlich.
function demonstrateConflicts(MessageQueueInterface $mq): void
{
    $typed = static function (T_UserCreated $event): void {};
    try {
        $mq->subscribe($typed, options: new SubscriptionOptions(topic: 'other.users'));
    } catch (MessageMappingException $error) {
        // MAPPING_CONFLICT: field=topic, MessageType=users, options=other.users.
        // Ablehnung vor Broker-Binding; kein stilles Überschreiben.
    }

    try {
        $mq->subscribe(static function (T_AuditEntry $entry): void {});
    } catch (MessageMappingException $error) {
        // MAPPING_INCOMPLETE: missingFields=[topic, subscription].
    }

    $subscription = $mq->subscribe($typed);
    try {
        try {
            $mq->subscribe($typed);
        } catch (InvalidHandlerException $error) {
            // DUPLICATE_SUBSCRIPTION: users/sdk-users bereits lokal registriert.
            // Derselbe Gruppenname in einem anderen Workerprozess ist dagegen erlaubt.
        }
    } finally {
        $subscription->cancel(); // Lokales Binding lösen; dauerhafte Gruppe bleibt bestehen.
    }
}
