<?php

declare(strict_types=1);

namespace Examples\MessageQueue\ProcessingWorkers;

use Phore\MessageQueue\ConnectionFactory;
use Phore\MessageQueue\ConnectionOptions;
use Phore\MessageQueue\MessageQueueInterface;
use Phore\MessageQueue\Rpc\CommandFailedException;
use Phore\MessageQueue\Rpc\RequestContext;
use Phore\MessageQueue\Rpc\RequestOptions;
use Phore\MessageQueue\Rpc\RpcConnectionOptions;
use Phore\MessageQueue\Security\HmacSecurity;
use Phore\MessageQueue\SubscriptionOptions;

/**
 * API-ENTWURF, noch nicht ausführbar. Proposal § 15.3.
 * Prozesse A/B/C: runWorker($dsn, $secret, 'worker-1'/'worker-2'/'worker-3').
 * Danach Prozess D: submitJobs($dsn, $secret).
 * Alle Connections nutzen denselben Redis-Prefix. Ein Reply-Endpunkt pro Client.
 */

function connect(string $dsn, string $secret, bool $client): MessageQueueInterface
{
    return (new ConnectionFactory())->connect($dsn, new ConnectionOptions(
        security: new HmacSecurity(
            sharedSecret: $secret,
            keyId: 'development-1',
            audience: 'processing-demo',
        ),
        rpc: new RpcConnectionOptions(
            replyTopic: $client ? 'jobs.replies.client-demo' : null,
            replySubscription: $client ? 'processing-client-demo' : null,
            allowedReplyTopics: ['jobs.replies.client-demo'],
        ),
        autoCreate: true,
    ));
}

function runWorker(string $dsn, string $secret, string $workerId): void
{
    $mq = connect($dsn, $secret, client: false);
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

function submitJobs(string $dsn, string $secret): void
{
    $mq = connect($dsn, $secret, client: true);
    try {
        $pending = [];
        foreach (['  erster Job  ', '  zweiter Job  ', '  dritter Job  '] as $text) {
            // Keine Worker-Adresse: der Broker wählt einen verfügbaren Consumer.
            $pending[] = $mq->request('jobs.text', 'text.process.v1', ['text' => $text],
                // Ursprüngliche Antwortfrist: 15 s ab request(), nicht ab await().
                new RequestOptions(timeoutSeconds: 15));
        }

        foreach ($pending as $call) {
            // Ohne lokalen Timeout wartet await nur bis zur ursprünglichen Deadline.
            // Die Fristen laufen seit dem Senden PARALLEL, nicht je weitere 15 s pro await.
            // Ohne rechtzeitiges finales Ergebnis: RequestTimeoutException (catch in 05).
            // Kein erneutes Senden, kein Abbruch des entfernten Workers durch den Timeout.
            $reply = $call->await(); // Dispatcher ordnet auch frühere Antworten korrekt zu.
            printf("%s: %s (%d Bytes), SHA-256 %s\n",
                $reply->metadata['app.workerId'],
                $reply->payload['normalized'],
                $reply->payload['bytes'],
                $reply->payload['sha256']);
        }
        // Jeder Job hat ein Ergebnis eines Workers. Es ist zulässig, dass ein
        // Worker mehrere Jobs erhält: Gleichverteilung/Zufall wird nicht garantiert.
    } finally {
        $mq->close();
    }
}

// Crash/Lease-Ablauf/Ack-Verlust können eine erneute Zustellung verursachen.
// Für schreibende Jobs zusätzlich Idempotenz/Fencing einsetzen; eine Gruppe
// allein garantiert keine Exactly-once-Ausführung. Timeout/Remote-Fehler wie in 05 behandeln.
