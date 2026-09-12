<?php

declare(strict_types=1);

namespace Examples\MessageQueue\CallbackErrors;

require_once __DIR__ . '/connection.php';

use function Examples\MessageQueue\demoConnection;

use Phore\MessageQueue\PhoreMQ;
use Phore\MessageQueue\MessageContext;
use Phore\MessageQueue\SubscriptionOptions;
use Phore\MessageQueue\Exception\RetryableMessageException;
use Phore\MessageQueue\Exception\RejectMessageException;
use Phore\MessageQueue\Exception\FailureStoreException;
use Phore\MessageQueue\Exception\ConnectionException;

/**
 * API-ENTWURF, noch nicht ausführbar. Verbindungsvorgaben: 01-connect.php.
 * demo() mit einem frischen Demo-Namespace aufrufen.
 * Dieses Beispiel verändert keine externen Daten; echte Jobs brauchen Idempotenz.
 */
function demo(): void
{
    $mq = new PhoreMQ(...demoConnection());
    try {
        $mq->subscribe('jobs.demo', 'demo-workers', function (array $job, MessageContext $context): void {
            if (!is_string($job['mode'] ?? null)) {
                // Dauerhaft ungültige Eingabe: keine Wiederholung; sichere Fehlerablage.
                throw new RejectMessageException('Das Pflichtfeld mode fehlt.');
            }
            if ($job['mode'] === 'temporary' && $context->attempt < 3) {
                // Simulierte temporäre Störung. Versuch 1 und 2 scheitern, 3 gelingt.
                throw new RetryableMessageException('Der Beispieldienst ist vorübergehend belegt.');
            }
            if ($job['mode'] === 'unexpected') {
                // Auch normale unbehandelte Exceptions werden begrenzt wiederholt.
                throw new \RuntimeException('Simulierter unerwarteter Handlerfehler.');
            }
            printf("Job %s erfolgreich, Versuch %d\n", $context->messageId, $context->attempt);
            // Erst erfolgreiche Rückkehr führt bei Auto-Ack zur Bestätigung.
            // Exception NICHT nur loggen und anschließend normal zurückkehren:
            // das würde die fehlgeschlagene Verarbeitung fälschlich bestätigen.
        }, new SubscriptionOptions(type: 'demo.process.v1'));

        $mq->publish('jobs.demo', 'demo.process.v1', ['mode' => 'temporary']);
        $mq->publish('jobs.demo', 'demo.process.v1', []);
        $mq->publish('jobs.demo', 'demo.process.v1', ['mode' => 'unexpected']);

        // Vorgeschlagener Standard: 1 erster Versuch + höchstens 3 Wiederholungen,
        // mit 1/2/4 Sekunden Verzögerung und ohne Jitter. Kein enger Requeue-Loop.
        // RetryableMessageException hebt die Obergrenze NICHT auf.
        // temporary: Erfolg im Versuch 3; fehlendes mode: sofort Fehlerablage;
        // unexpected: nach Versuch 4 Fehlerablage. Insgesamt 8 Zustellversuche.
        // Das ist keine Reihenfolgegarantie. Verarbeitung anderer Jobs läuft weiter.
        // maxSeconds beendet normal; es garantiert nicht, dass alle Retries fertig sind.
        $mq->run(maxMessages: 8, maxSeconds: 30);
        // Der RabbitMQ-Adapter speichert endgültige Fehler bestätigt in der Fehlerqueue.
        // In Produktion nach Ursachenbehebung gezielt redriven, nicht blind neu senden.
    } catch (FailureStoreException | ConnectionException $infrastructureError) {
        // Infrastrukturfehler sind anders als behandelte Callback-Fehler:
        // run wirft zum Prozessbetreiber zurück; fehlgeschlagene Ablage => KEIN Ack.
        // Lokal redigiert protokollieren und den Supervisor kontrolliert reagieren lassen.
        error_log('Queue-Infrastruktur nicht verfügbar; Ursache lokal untersuchen.');
        throw $infrastructureError;
    } finally {
        $mq->close();
    }
}

// RPC: respond() + CommandFailedException erzeugt eine sichere finale Fehlerantwort;
// await() wirft dann RemoteCommandException. Callback-Retry bleibt davon getrennt.
// Nach erschöpften technischen Retries: generischer sicherer HANDLER_FAILED-Fehler,
// soweit der Rückkanal vor Deadline erreichbar ist; andernfalls ggf. Client-Timeout.
// Vollständiger RPC-Code samt RequestTimeoutException: 05-rpc.php.
// Middleware zum Protokollieren und WEITERWERFEN der Originalexception: 06-metadata-middleware.php.
// Nach manuellem ack() kann eine später geworfene Exception dieses Ack nicht rückgängig machen.
