<?php

declare(strict_types=1);

namespace Examples\MessageQueue\Metadata;

use Phore\MessageQueue\ConnectionFactory;
use Phore\MessageQueue\ConnectionOptions;
use Phore\MessageQueue\Exception\RejectMessageException;
use Phore\MessageQueue\MessageContext;
use Phore\MessageQueue\Middleware\OutgoingMessage;
use Phore\MessageQueue\PublishOptions;
use Phore\MessageQueue\PublishReceipt;
use Phore\MessageQueue\RunOptions;
use Phore\MessageQueue\Security\HmacSecurity;
use Phore\MessageQueue\SubscriptionOptions;

/**
 * API-ENTWURF, noch nicht ausführbar. Proposal § 14.
 * Zwei optionale Callable-Hooks, keine Middleware-Basisklasse erforderlich.
 * Beispielaufruf: demo($dsn, $secret, $traceId), mit frischem Redis-Prefix.
 */

function demo(string $dsn, string $secret, string $traceId): void
{
    $factory = new ConnectionFactory();
    $security = new HmacSecurity(
        sharedSecret: $secret,
        keyId: 'development-1',
        audience: 'orders-development',
    );

    // Separate Connection ohne Diagnose-Middleware verhindert Fehlerschleifen.
    $diagnostics = $factory->connect($dsn, new ConnectionOptions(
        security: $security,
        autoCreate: true,
    ));

    try {
        $mq = $factory->connect($dsn, new ConnectionOptions(
            security: $security,
            autoCreate: true,
            sendMiddleware: [
                // $next: callable(OutgoingMessage): PublishReceipt
                static function (OutgoingMessage $message, callable $next) use ($traceId): PublishReceipt {
                    // Ergänzt nur app.*-Metadaten; Payload bleibt fachlich unverändert.
                    // Signierung erfolgt nach diesem Hook.
                    return $next($message->withMetadata(['app.traceId' => $traceId]));
                },
            ],
            handleMiddleware: [
                // $next: callable(mixed, MessageContext): mixed
                static function (mixed $payload, MessageContext $context, callable $next) use ($diagnostics): mixed {
                    try {
                        // Rückgabe durchreichen: funktioniert auch um RPC-Responder.
                        return $next($payload, $context);
                    } catch (\Throwable $original) {
                        try {
                            $diagnostics->publish('diagnostics', 'diagnostic.v1', [
                                'level' => 'error',
                                'code' => 'HANDLER_FAILED',
                                'message' => 'Eine Nachricht konnte nicht verarbeitet werden.',
                            ], new PublishOptions(
                                correlationId: $context->correlationId ?? $context->messageId,
                                metadata: [
                                    'app.traceId' => $context->metadata['app.traceId'] ?? 'unknown',
                                    'app.sourceTopic' => $context->topic,
                                ],
                            ));
                        } catch (\Throwable) {
                            // Lokaler, redigierter Fallback; keine rekursive MQ-Meldung.
                            error_log('Die MQ-Diagnosemeldung konnte nicht versendet werden.');
                        }
                        // Ursprüngliche Ausnahme bewahren: kein fälschliches Erfolgs-Ack.
                        throw $original;
                    }
                },
            ],
        ));

        try {
            $diagnostics->subscribe('diagnostics', 'diagnostic-viewer', static function (array $notice, MessageContext $context): void {
                printf("%s [%s], Bezug %s\n", $notice['level'], $notice['code'], $context->correlationId);
            }, new SubscriptionOptions(type: 'diagnostic.v1'));

            $mq->subscribe('orders', 'order-workers', static function (array $order, MessageContext $context): void {
                // Separat zugänglich, nicht in $order integriert.
                printf("Auftrag %s, Sprache %s, Trace %s\n",
                    $order['orderId'],
                    $context->metadata['app.locale'] ?? 'en',
                    $context->metadata['app.traceId']);

                if (($order['mode'] ?? '') === 'reject') {
                    throw new RejectMessageException('Der Beispielauftrag wurde abgelehnt.');
                }
            }, new SubscriptionOptions(type: 'order.submit.v1'));

            // Einfacher Event-Aufruf mit frei gewählten, begrenzten Anwendungsmetadaten.
            $mq->publish('orders', 'order.submit.v1', ['orderId' => 'order-42'], new PublishOptions(
                correlationId: 'request-42',
                metadata: ['app.locale' => 'de-DE'],
            ));
            // Höchstens 1 Zustellversuch(e) insgesamt oder 5 s Gesamtbudget; erstes Limit gewinnt.
            // Normale Rückkehr, keine Mindestzahl/Timeout-Exception; Details in 02-programmatic.php.
            $mq->run(new RunOptions(maxMessages: 1, maxSeconds: 5));

            // Eine Warning direkt als gewöhnliches Event versenden: keine neue API nötig.
            $diagnostics->publish('diagnostics', 'diagnostic.v1', [
                'level' => 'warning',
                'code' => 'OPTIONAL_DATA_MISSING',
                'message' => 'Optionale Auftragsdaten fehlen.',
            ], new PublishOptions(
                correlationId: 'request-42',
                metadata: ['app.traceId' => $traceId],
            ));
            // Höchstens 1 Zustellversuch(e) insgesamt oder 5 s Gesamtbudget; erstes Limit gewinnt.
            // Normale Rückkehr, keine Mindestzahl/Timeout-Exception; Details in 02-programmatic.php.
            $diagnostics->run(new RunOptions(maxMessages: 1, maxSeconds: 5));

            // Für den Fehlerpfad oben: payload ['orderId' => 'order-43', 'mode' => 'reject'].
            // Der Handlerfehler bleibt erhalten; die Middleware sendet separat level=error.
            // RPC-Begleitmeldungen am Rückkanal zeigt zusätzlich Beispiel 05.
        } finally {
            $mq->close();
        }
    } finally {
        $diagnostics->close();
    }
}
