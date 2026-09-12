<?php

declare(strict_types=1);

namespace Examples\MessageQueue\Rpc;

use Phore\MessageQueue\Attribute\Respond;
use Phore\MessageQueue\ConnectionFactory;
use Phore\MessageQueue\ConnectionOptions;
use Phore\MessageQueue\MessageQueueInterface;
use Phore\MessageQueue\Rpc\CommandFailedException;
use Phore\MessageQueue\Rpc\Notice;
use Phore\MessageQueue\Rpc\RemoteCommandException;
use Phore\MessageQueue\Rpc\RequestContext;
use Phore\MessageQueue\Rpc\RequestOptions;
use Phore\MessageQueue\Rpc\RequestTimeoutException;
use Phore\MessageQueue\Rpc\RpcConnectionOptions;
use Phore\MessageQueue\Security\HmacSecurity;
use Phore\MessageQueue\SubscriptionOptions;

/**
 * API-ENTWURF, noch nicht ausführbar. Proposal §§ 13–14.
 * Nach Implementierung: zuerst runServer($dsn, $secret) in Prozess A starten,
 * dann runClient($dsn, $secret) in Prozess B. Beide nutzen denselben Redis-Prefix.
 * replyTopic/replySubscription müssen je gleichzeitig aktiver Client-Instanz
 * eindeutig sein. Die festen Namen unten gelten für genau einen Demo-Client.
 */

function connect(string $dsn, string $secret, bool $client): MessageQueueInterface
{
    return (new ConnectionFactory())->connect($dsn, new ConnectionOptions(
        security: new HmacSecurity(
            sharedSecret: $secret,
            keyId: 'development-1',
            audience: 'rpc-development',
        ),
        rpc: new RpcConnectionOptions(
            replyTopic: $client ? 'rpc.replies.client-demo' : null,
            replySubscription: $client ? 'client-demo' : null,
            allowedReplyTopics: ['rpc.replies.client-demo'],
        ),
        autoCreate: true, // Produktion: Ressourcen und ACLs vorher provisionieren.
    ));
}

final class DivideHandler
{
    // Alternative zur programmatischen Registrierung unten: registerHandlers().
    #[Respond(topic: 'calculator', subscription: 'calculator-workers', type: 'math.divide.v1')]
    public function divide(array $params, RequestContext $request): array
    {
        // Array-Modus: fachliche Parameterprüfung explizit im Handler.
        // Optional kann hier stattdessen ein lokales Parameter-DTO stehen.
        foreach (['a', 'b'] as $name) {
            if (!isset($params[$name]) || (!is_int($params[$name]) && !is_float($params[$name]))) {
                throw new CommandFailedException(
                    errorCode: 'INVALID_ARGUMENT',
                    publicMessage: 'Die Parameter a und b müssen Zahlen sein.',
                );
            }
        }

        if ((float) $params['b'] === 0.0) {
            // Erzeugt eine terminale, sichere Fehlerantwort am Rückkanal.
            throw new CommandFailedException(
                errorCode: 'DIVIDE_BY_ZERO',
                publicMessage: 'Division durch null ist nicht möglich.',
            );
        }

        if (abs($params['b']) < 1) {
            // Sofortige Begleitmeldung; Rückgabewert bleibt davon unabhängig.
            $request->notify(new Notice(
                level: 'warning',
                code: 'SMALL_DIVISOR',
                message: 'Der Divisor ist kleiner als eins.',
            ));
        }

        $request->setReplyMetadata(['app.worker' => 'calculator-v1']);
        return ['quotient' => $params['a'] / $params['b']];
    }
}

function runServer(string $dsn, string $secret): void
{
    $mq = connect($dsn, $secret, client: false);
    try {
        $mq->respond('calculator', 'calculator-workers', [new DivideHandler(), 'divide'],
            new SubscriptionOptions(type: 'math.divide.v1'));

        // Gleichwertige Alternative mit Attributen, NICHT zusätzlich registrieren:
        // $mq->registerHandlers(new DivideHandler());
        $mq->run();
    } finally {
        $mq->close();
    }
}

function runClient(string $dsn, string $secret): void
{
    $mq = connect($dsn, $secret, client: true);
    try {
        // Kleiner Standardaufruf: Parameter senden und ausdrücklich auf Antwort warten.
        $reply = $mq->request('calculator', 'math.divide.v1', ['a' => 12, 'b' => 3])->await();
        printf("Ergebnis: %s\n", $reply->payload['quotient']); // 4

        // Erweiterter Aufruf: Metadaten außerhalb der Parameter, Warnings live.
        $pending = $mq->request('calculator', 'math.divide.v1', ['a' => 12, 'b' => 0.5], new RequestOptions(
            timeoutSeconds: 5,
            metadata: ['app.traceId' => 'trace-demo-42', 'app.locale' => 'de-DE'],
            onNotice: static function (Notice $notice): void {
                printf("%s [%s]: %s\n", $notice->level, $notice->code, $notice->message);
            },
        ));
        // Hier kann der Client andere Arbeit erledigen; erst await() blockiert.
        $reply = $pending->await();
        printf("Ergebnis: %s, Worker: %s\n", $reply->payload['quotient'], $reply->metadata['app.worker']);
        // $reply->notices enthält die finale Zusammenfassung; nicht doppelt ausgeben.
        // Optional: RequestOptions(responseClass: LocalResult::class), wenn die
        // Connection eine PhoreSchemaMapper-Bridge verwendet; gleiches Strukturprinzip.

        try {
            $mq->request('calculator', 'math.divide.v1', ['a' => 12, 'b' => 0])->await();
        } catch (RemoteCommandException $error) {
            printf("Command fehlgeschlagen [%s]: %s\n", $error->errorCode, $error->getMessage());
            // Erwartet: DIVIDE_BY_ZERO; keine entfernten PHP-Stacks/Objekte.
        }
    } catch (RequestTimeoutException $timeout) {
        // Ein Timeout stoppt das entfernte Command NICHT und sendet es nicht erneut.
        printf("Keine rechtzeitige Antwort für Request %s\n", $timeout->requestId);
    } finally {
        $mq->close();
    }
}
