<?php

declare(strict_types=1);

namespace Examples\MessageQueue\ProcessingWorkers;

require_once __DIR__ . '/connection.php';

use function Examples\MessageQueue\demoConnection;

use Phore\MessageQueue\PhoreMQ;
use Phore\MessageQueue\Rpc\CommandFailedException;
use Phore\MessageQueue\Rpc\RequestContext;
use Phore\MessageQueue\PublishOptions;
use Phore\MessageQueue\SubscriptionOptions;
use Phore\MessageQueue\Rpc\RemoteCommandException;
use Phore\MessageQueue\Rpc\RequestTimeoutException;

/**
 * API-ENTWURF, noch nicht ausführbar. Proposal § 15.3.
 * Prozesse A/B/C: runWorker( 'worker-1'/'worker-2'/'worker-3').
 * Danach Prozess D: submitJobs().
 * Alle Connections nutzen denselben RabbitMQ-Namespace. Ein Reply-Endpunkt pro Client.
 */

function runWorker(string $workerId): void
{
    $mq = new PhoreMQ(...demoConnection());
    try {
        // ENTSCHEIDEND: Alle Worker verwenden exakt dieselbe Subscription.
        // $workerId NICHT an 'text-processors' anhängen, sonst entsteht Fan-out!
        // Transport-Consumer-IDs vergibt die Connection unabhängig voneinander.
        $mq->respond('jobs.text', 'text-processors',
            static function (array $job, RequestContext $request) use ($workerId): array {
                if (!is_string($job['text'] ?? null)) {
                    throw new CommandFailedException(
                        errorCode: 'INVALID_TEXT',
                        publicMessage: 'Der Job benötigt das String-Feld text.',
                    );
                }
                $text = trim($job['text']);
                $request->setReplyMetadata(['app.workerId' => $workerId]);

                // Reine, wiederholbare Verarbeitung ohne externe Seiteneffekte.
                return ['normalized' => $text, 'bytes' => strlen($text), 'sha256' => hash('sha256', $text)];
            }, new SubscriptionOptions(type: 'text.process.v1'));
        $mq->run();
    } finally {
        $mq->close();
    }
}

function submitJobs(): void
{
    $mq = new PhoreMQ(...demoConnection());
    try {
        $pending = [];
        foreach (['  erster Job  ', '  zweiter Job  ', '  dritter Job  '] as $text) {
            // Keine Worker-Adresse: der Broker wählt einen verfügbaren Consumer.
            $pending[] = $mq->request('jobs.text', 'text.process.v1', ['text' => $text],
                // Ursprüngliche Antwortfrist: 15 s ab request(), nicht ab await().
                new PublishOptions(replyTimeoutSeconds: 15));
        }

        foreach ($pending as $call) {
            // Ohne lokalen Timeout wartet await nur bis zur ursprünglichen Deadline.
            // Die Fristen laufen seit dem Senden PARALLEL, nicht je weitere 15 s pro await.
            // Ohne rechtzeitiges finales Ergebnis: RequestTimeoutException (catch in 05).
            // Kein erneutes Senden, kein Abbruch des entfernten Workers durch den Timeout.
            try {
                $reply = $call->await(); // Antwort zu genau diesem bereits gesendeten Auftrag.
            } catch (RequestTimeoutException | RemoteCommandException $error) {
                printf("Auftrag ohne Erfolgsergebnis: %s\n", $error->getMessage());
                continue; // Andere Aufträge sind bereits gesendet und werden weiter abgeholt.
            }
            printf("%s: %s (%d Bytes), SHA-256 %s\n",
                $reply->metadata['app.workerId'],
                $reply->payload['normalized'],
                $reply->payload['bytes'],
                $reply->payload['sha256']);
        }
        // Jede erfolgreiche Antwort stammt von einem Worker. Es ist zulässig, dass ein
        // Worker mehrere Jobs erhält: Gleichverteilung/Zufall wird nicht garantiert.
    } finally {
        $mq->close();
    }
}

// Crash/Verbindungsabbruch/Ack-Verlust können eine erneute Zustellung verursachen.
// Für schreibende Jobs zusätzlich Idempotenz/Fencing einsetzen; eine Gruppe
// allein garantiert keine Exactly-once-Ausführung. Timeout/Remote-Fehler wie in 05 behandeln.
