<?php

declare(strict_types=1);

namespace Examples\MessageQueue\QueueConfiguration;

require_once __DIR__ . '/connection.php';

use function Examples\MessageQueue\demoConnection;
use Phore\MessageQueue\Attribute\MessageType;
use Phore\MessageQueue\Attribute\Queue;
use Phore\MessageQueue\Attribute\Respond;
use Phore\MessageQueue\ConnectionOptions;
use Phore\MessageQueue\PhoreMQ;
use Phore\MessageQueue\QueueOptions;
use Phore\MessageQueue\QueueProfile;
use Phore\MessageQueue\SubscriptionOptions;
use Phore\MessageQueue\Exception\QueueConfigurationConflictException;
use Phore\MessageQueue\Exception\RetryableMessageException;
use Phore\MessageQueue\Rpc\CommandFailedException;

// API-ENTWURF für PHP >=8.5; keine dieser Library-Klassen ist implementiert.
// Optionen gehören der Subscription; publish provisioniert auch bei DTOs nichts.
#[MessageType('text.normalize.v1', topic: 'text', subscription: 'text-workers')]
#[Queue(profile: QueueProfile::Rpc, revision: 1, retentionSeconds: 3600,
    maxAttempts: 4, retryDelaySeconds: 15)]
final class NormalizeText
{
    public function __construct(public string $text) {}
}

final class TextHandler
{
    // Topic, Subscription, Typ UND Queue-Vorgaben aus dem ersten DTO-Parameter.
    // Strukturelle Hydration benötigt die Schema-Bridge; keine doppelten Angaben.
    #[Respond]
    public function normalize(NormalizeText $command): array
    {
        if ($command->text === '') {
            // Permanent: Eingabe korrigieren. Keine Retry-Runde.
            // Client-await wirft RemoteCommandException mit dieser sicheren Meldung.
            throw new CommandFailedException('EMPTY_TEXT', 'Der Text darf nicht leer sein.');
        }
        return ['text' => trim($command->text)];
    }
}

function runTypedWorker(): void
{
    $mq = new PhoreMQ(...demoConnection());
    try {
        $mq->registerHandlers(new TextHandler());
        // Gleichwertige Alternative, nicht zusätzlich registrieren:
        // $mq->respond([new TextHandler(), 'normalize']);
        $mq->run(); // Empfangsschleife bis stop()/Infrastrukturfehler, sendet nichts.
    } finally {
        $mq->close();
    }
}

function runProgrammaticWorker(): void
{
    $mq = new PhoreMQ(...demoConnection());
    try {
        // Alternative zum typisierten Worker, identischer gemeinsamer Queue-Contract.
        $mq->respond('text', 'text-workers', static function (array $command): array {
            if (!is_string($command['text'] ?? null) || $command['text'] === '') {
                throw new CommandFailedException('EMPTY_TEXT', 'Ein nicht leerer Text wird benötigt.');
            }
            return ['text' => trim($command['text'])];
        }, new SubscriptionOptions(type: 'text.normalize.v1', queue: QueueOptions::rpc(
            revision: 1,
            retentionSeconds: 3600, // Maximal 1 h wartend; Ack entfernt früher, kein Handler-Timeout.
            maxInFlight: 1, // Ein offener Auftrag je Consumer, keine erzeugten Threads.
            maxAttempts: 4, // Erstversuch + maximal drei reguläre Handler-Retries.
            retryDelaySeconds: 15, // Mindestens 15 s bis erneut verfügbar; keine Ausführungsgarantie exakt dann.
        )));
        $mq->run();
    } catch (QueueConfigurationConflictException $error) {
        // Auch QueueMigrationRequiredException fällt hierunter.
        // Beispielsweise: option=profile, actual=broadcast, requested=rpc.
        // Keine automatische Löschung oder Änderung existierender Queues.
        printf("Queue %s/%s: Option %s ist inkompatibel (%s -> %s)\n",
            $error->topic, $error->subscription, $error->option,
            json_encode($error->actual, JSON_THROW_ON_ERROR),
            json_encode($error->requested, JSON_THROW_ON_ERROR));
        throw $error; // Kein scheinbar erfolgreich gestarteter Listener.
    } finally {
        $mq->close();
    }
}

function runEvents(): void
{
    $mq = new PhoreMQ(...demoConnection());
    try {
        $mq->subscribe('live', 'screen', static function (array $event): void {
            echo $event['text'];
        }, new SubscriptionOptions(queue: QueueOptions::broadcast()));
        // Jede registrierte Connection bekommt eine eigene flüchtige Kopie.
        // Keine Offline-Aufbewahrung, keine Handler-Retries; nicht für Pflicht-Jobs.
        // retentionSeconds: 300 würde eine dauerhafte Subscription mit maximal
        // 5 Minuten Wartezeit erzeugen. Dafür jeder Empfängergruppe eigenen Namen
        // geben; gleiche Namen teilen dann Arbeit. Kein Replay für neue Gruppen.
        $mq->run();
    } finally {
        $mq->close();
    }
}

function runWithDefaults(): void
{
    // Dieses Beispiel zeigt ausdrücklich globale OPTIONS, siehe Verbindung in 01.
    $mq = new PhoreMQ(...demoConnection(new ConnectionOptions(
        queueDefaults: new QueueOptions(maxAttempts: 4, retryDelaySeconds: 10),
    )));
    try {
        $mq->subscribe('tasks', 'processors', static function (array $job): void {
            // Nur Demo einer temporären Störung; nach vier Runden Fehlerablage.
            throw new RetryableMessageException('Ressource ist vorübergehend gesperrt.');
        }, new SubscriptionOptions(queue: QueueOptions::workQueue()));
        // Defaults füllen offene Felder. Feste DTO-Werte dürfen nicht widersprüchlich
        // überschrieben werden. Broadcast benötigt maxAttempts=1, daher hier keine
        // globalen Work-Retrywerte unbesehen auf Broadcast anwenden.
        $mq->run(maxMessages: 4, maxSeconds: 60);
        // Höchstens vier Zustellversuche oder 60 s Gesamtbudget, normale Rückkehr.
        // Keine Mindestanzahl; spontane Redeliveries können dasselbe attempt wiederholen.
    } finally {
        $mq->close();
    }
}
