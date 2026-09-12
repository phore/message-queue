<?php

declare(strict_types=1);

namespace Examples\MessageQueue\Programmatic;

require_once __DIR__ . '/connection.php';

use function Examples\MessageQueue\demoConnection;
use Phore\MessageQueue\PhoreMQ;
use Phore\MessageQueue\PublishOptions;
use Phore\MessageQueue\ConnectionOptions;
use Phore\MessageQueue\Exception\MessageValidationException;
use Phore\MessageQueue\Exception\QueueConfigurationMissingException;
use Phore\MessageQueue\MessageContext;
use Phore\MessageQueue\MessageRegistry;
use Phore\MessageQueue\MessageQueueInterface;
use Phore\MessageQueue\SubscriptionOptions;
use Phore\MessageQueue\RunOptions;

// API-ENTWURF, PHP >=8.5; keine ausführbare MQ-Library. Proposal §§ 5–6, 11.
// Anwendung: zuerst runUserWorker() als Worker starten; danach publishUserCreated()
// im HTTP-Backend. Beide verwenden dieselbe zentrale Verbindungskonfiguration.
final class LocalUserCreated
{
    public string $userId;
    public string $email;
}

function runUserWorker(): void
{
    $registry = new MessageRegistry();
    $registry->register('user.created.v1', LocalUserCreated::class, topic: 'users');
    $mq = new PhoreMQ(...demoConnection(new ConnectionOptions(registry: $registry)));
    try {
        $mq->subscribe('users', 'audit-users', static function (array $event, MessageContext $context): void {
            printf("Audit: %s / %s\n", $context->messageId, $event['userId']);
        }, new SubscriptionOptions(type: 'user.created.v1'));

        $mq->subscribe('users', 'billing-users', static function (LocalUserCreated $event): void {
            printf("Billing: %s / %s\n", $event->userId, $event->email);
        }, new SubscriptionOptions(type: 'user.created.v1'));
        // Beide Gruppen erhalten eine Kopie. DTO-Hydration nutzt den Parameter-Typ;
        // der Sender darf ein Array oder eine andere strukturell passende Klasse senden.
        $mq->run(); // Erst jetzt Callbacks ausführen; Erfolgsrückkehr bestätigt die Kopie.
    } finally {
        $mq->close();
    }
}

function publishUserCreated(string $userId, string $email): void
{
    $mq = new PhoreMQ(...demoConnection());
    try {
        $mq->publish('users', 'user.created.v1', ['userId' => $userId, 'email' => $email], options: new PublishOptions(reply: false));
        // Rückkehr bestätigt Broker-Annahme; beide Worker können noch arbeiten.
    } catch (QueueConfigurationMissingException $error) {
        printf("Queue-Konfiguration fehlt für %s / %s (%s).\n",
            $error->topic, $error->messageType, $error->reason);
        // HTTP-Backend bildet diesen Zustand auf seine Fehlerantwort ab.
        // Keine automatische Anlage durch den Publisher; kein stiller Erfolg.
        throw $error;
    } finally {
        $mq->close();
    }
}

// Alternative Sendeseite mit Schema-Prüfung; injizierte MQ wurde wie im Worker
// mit Registry konfiguriert. Diese Funktion besitzt/schließt die Connection nicht.
function publishValidatedUser(MessageQueueInterface $mq, string $userId, string $email): void
{
    $event = new LocalUserCreated();
    $event->userId = $userId;
    $event->email = $email;
    $mq->publish($event, options: new PublishOptions(reply: false)); // Registry liefert Topic/Typ; erforderliche Felder prüfen, dann senden.
}

function demonstrateValidationFailure(MessageQueueInterface $mq): void
{
    try {
        $mq->publish('users', 'user.created.v1', ['userId' => 'missing-email'], options: new PublishOptions(reply: false));
    } catch (MessageValidationException $error) {
        // Nur mit registriertem Contract: user.created.v1 / $.email / required.
        // Validierung vor Publish; diese ungültige Nachricht wurde nicht gesendet.
        printf("Ungültige Nachricht: %s\n", $error->getMessage());
    }
}

// Weitere unabhängige Subscription ohne DTO/Schema; zuerst registrieren, dann run.
function runTelemetryWorker(): void
{
    $mq = new PhoreMQ(...demoConnection());
    try {
        $mq->subscribe('telemetry', 'audit-telemetry', static function (array $event): void {
            printf("Telemetry: %s\n", json_encode($event, JSON_THROW_ON_ERROR));
        }); // Kein type-Filter: Handler muss alle Typen des Topics verarbeiten können.
        $mq->run();
    } finally {
        $mq->close();
    }
}

// Separate Laufzeitvariante für eine bereits registrierte, injizierte Connection.
function processBatch(MessageQueueInterface $mq): void
{
    $mq->run(maxMessages: 100, maxSeconds: 30, idleTimeoutSeconds: 2);
    // maxMessages: höchstens 100 abgeschlossene Zustellversuche über alle Gruppen,
    // inklusive Retry/Reject; nicht 100 erfolgreiche/eindeutige Geschäftsoperationen.
    // maxSeconds: 30 s Gesamtbudget inklusive Warten und Handlerlaufzeit.
    // idleTimeoutSeconds: Ende nach 2 s Warten ohne fachliche Zustellung.
    // Erstes Limit gewinnt; normale Rückkehr auch bei weniger/keinen Nachrichten.
    // Ein synchroner Handler darf fertig werden und das Zeitbudget überschreiten.
    // Kein minMessages; eine erforderliche Anzahl prüft die Anwendung selbst.
    // Ohne Limits läuft run() bis stop() oder Infrastrukturfehler.
}

function processBatchWithOptions(MessageQueueInterface $mq, RunOptions $options): void
{
    $mq->run($options, maxMessages: 100); // Direkter Wert ersetzt nur dieses Optionsfeld.
    // Alternatives Beispiel, nicht zusätzlich processBatch() aufrufen.
    // Optionsobjekte bleiben unverändert; alle gesetzten Limits müssen positiv sein.
}
