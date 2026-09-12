<?php

declare(strict_types=1);

namespace Examples\MessageQueue\Rpc;

require_once __DIR__ . '/connection.php';

use function Examples\MessageQueue\demoConnection;

use Phore\MessageQueue\PhoreMQ;
use Phore\MessageQueue\Attribute\Respond;
use Phore\MessageQueue\Attribute\RemoteError;
use Phore\MessageQueue\Rpc\RemoteException;
use Phore\MessageQueue\Attribute\MessageType;
use Phore\MessageQueue\PublishOptions;
use Phore\MessageQueue\RunOptions;
use Phore\MessageQueue\Rpc\AwaitOptions;
use Phore\MessageQueue\MessageQueueInterface;
use Phore\MessageQueue\Rpc\CommandFailedException;
use Phore\MessageQueue\Rpc\Notice;
use Phore\MessageQueue\Rpc\RemoteCommandException;
use Phore\MessageQueue\Rpc\RequestContext;
use Phore\MessageQueue\Rpc\RequestTimeoutException;
use Phore\MessageQueue\SubscriptionOptions;

/**
 * API-ENTWURF, noch nicht ausführbar. Proposal §§ 13–14.
 * Nach Implementierung: zuerst runServer() in Prozess A starten,
 * dann runClient() in Prozess B. Beide nutzen denselben RabbitMQ-Namespace.
 * Der RabbitMQ-Adapter vergibt pro Client einen eigenen Rückkanal; Details in 01-connect.php.
 */

#[MessageType('math.divide.v1', topic: 'calculator')]
final class Divide
{
    public function __construct(public float $a, public float $b) {}
}

// Dieser explizite Fehlertyp darf seine Meldung über den RPC-Rückkanal senden.
// In einem gemeinsamen SDK können Server und Client dieselbe Klasse verwenden.
#[RemoteError('math.division_by_zero.v1')]
final class DivisionByZero extends RemoteException {}

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
            throw new DivisionByZero('Division durch null ist nicht möglich.', errorCode: 'DIVIDE_BY_ZERO');
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

function runServer(): void
{
    $mq = new PhoreMQ(...demoConnection());
    try {
        $mq->respond('calculator', 'calculator-workers', [new DivideHandler(), 'divide'],
            new SubscriptionOptions(type: 'math.divide.v1'));

        // Gleichwertige Alternative mit Attributen, NICHT zusätzlich registrieren:
        // $mq->registerHandlers(new DivideHandler());
        // maxMessages: maximal 100 Zustellversuche insgesamt, nicht je Subscription.
        // maxSeconds: bis zu 60 s Gesamtbudget inklusive Leerlauf; erstes Limit gewinnt.
        // Ein laufender synchroner Handler darf noch fertig werden, ggf. über die 60 s.
        // Erreichen dieser Grenzen ist normale Rückkehr, keine Timeout-Exception.
        // minMessages ist nicht vorgesehen; weniger als 100 Versuche sind zulässig.
        $mq->run(maxMessages: 100, maxSeconds: 60);
        // Alternativen: run(new RunOptions(maxMessages: 100, maxSeconds: 60))
        // oder run(new RunOptions(maxSeconds: 60), maxMessages: 100).
        // Direkte Werte überschreiben dieselben Optionsfelder, ohne das Objekt zu ändern.
    } finally {
        $mq->close();
    }
}

function runClient(): void
{
    $mq = new PhoreMQ(...demoConnection());
    try {
        // Publish erkennt das DTO und übernimmt Topic/Typ. Sendet sofort.
        $sent = $mq->publish(new Divide(12, 3));
        // Andere lokale Arbeit wäre hier möglich; der Broker hat die Nachricht bereits.
        // timeoutSeconds: maximal 5 s LOKALES Warten ab diesem await-Aufruf,
        // begrenzt durch die verbleibende, schon beim Publish gesetzte Antwortfrist.
        // Bereits vorhandene finale Antwort: sofortige Rückgabe ohne neue Sendung.
        // Kein finales Result/Error bis dahin: RequestTimeoutException (catch unten),
        // niemals null/false oder ein leeres scheinbar erfolgreiches Reply.
        $reply = $sent->await(timeoutSeconds: 5);
        printf("Ergebnis: %s\n", $reply->payload['quotient']); // 4

        // Gleicher Aufruf in einer Zeile; await sendet NICHT erneut.
        $reply = $mq->publish(new Divide(15, 3))->await(timeoutSeconds: 5);

        // Ohne Warten: ignoriertes SendResult verzögert oder verhindert das Senden nicht.
        $mq->publish(new Divide(20, 4));
        // Der Responder arbeitet trotzdem; seine nicht benötigte Antwort läuft ab/wird verworfen.

        // Programmatische Variante, Metadaten vor Publish, Warteoptionen erst bei await.
        $pending = $mq->publish('calculator', 'math.divide.v1', ['a' => 12, 'b' => 0.5], new PublishOptions(
            reply: true, // Rückkanal zwingend: fehlende Konfiguration scheitert VOR Publish.
            replyTimeoutSeconds: 15, // Wire-Deadline ab Senden; await verlängert sie nicht.
            metadata: ['app.traceId' => 'trace-demo-42', 'app.locale' => 'de-DE'],
        ));
        $reply = $pending->await(new AwaitOptions(timeoutSeconds: 10),
            timeoutSeconds: 5, // Direkter Wert überschreibt hier die 10 Sekunden.
            // onNotice: Zwischenmeldungen ausgeben; sie beenden await nicht und
            // setzen weder die lokale Wartefrist noch die Remote-Deadline zurück.
            onNotice: static function (Notice $notice): void {
                printf("%s [%s]: %s\n", $notice->level, $notice->code, $notice->message);
            },
        );
        printf("Ergebnis: %s, Worker: %s\n", $reply->payload['quotient'], $reply->metadata['app.worker']);
        // reply->notices enthält die finale Zusammenfassung; nicht doppelt ausgeben.
        // responseClass: lokale DTO-Klasse für reply->payload; vor Rückgabe strukturell
        // prüfen/hydrieren. Ohne Angabe Array; benötigt bei DTOs die Schema-Bridge.
        // Beispiel: await(responseClass: LocalResult::class).
        // Reine Events können mit PublishOptions(reply: false) ohne Antwortaufwand senden.

        // 1. Allgemeine Fehlerbehandlung: kein Fehler-Topic abonnieren erforderlich.
        try {
            $mq->publish(new Divide(12, 0))->await(timeoutSeconds: 5);
        } catch (RemoteCommandException $error) {
            // Kein lokales Mapping: generische Exception, aber gleiche freigegebene Meldung.
            printf("Remote-Fehler [%s]: %s\n", $error->errorCode, $error->getMessage());
        }

        // 2. Gewünschte lokale Exception-Klasse ausdrücklich freigeben.
        try {
            $mq->publish(new Divide(12, 0))->await(
                timeoutSeconds: 5,
                errorTypes: [DivisionByZero::class],
            );
        } catch (DivisionByZero $error) {
            // Gleiche SDK-Klasse und Meldung wie auf dem Server, lokal neu erzeugt.
            printf("Division korrigieren: %s\n", $error->getMessage());
        } catch (RemoteCommandException $error) {
            // Unbekannter anderer Fehler bleibt ein generischer Remote-Fehler.
            printf("RPC fehlgeschlagen: %s\n", $error->getMessage());
        }
        // Alternativ await(new AwaitOptions(errorTypes: [DivisionByZero::class])).
        // Der Client darf auch eine andere lokale Klasse mit demselben RemoteError-Namen
        // registrieren. Keine entfernten PHP-Klassennamen, Stacktraces oder unserialize().

    } catch (RequestTimeoutException $timeout) {
        // await wirft RequestTimeoutException bei abgelaufener lokaler Wartefrist
        // oder ursprünglicher Antwortdeadline ohne rechtzeitig empfangenes finales Ergebnis.
        // Der Fehler enthält requestId. Er ist kein RemoteCommandException:
        // ein bekannter fachlicher Fehler des Responders ist eine andere Ursache.
        // Ein Timeout stoppt das entfernte Command NICHT und sendet es nicht erneut.
        // Falls die ursprüngliche Antwortfrist noch läuft, kann derselbe SendResult
        // erneut await() aufrufen. Nicht publish() wiederholen: das wäre ein neuer Job.
        printf("Keine rechtzeitige Antwort für Request %s\n", $timeout->requestId);
    } finally {
        $mq->close();
    }
}

// request(...)->await() bleibt eine explizite RPC-Komfortform (Beispiel 08).
// subscribe-Handler antworten nicht automatisch: Ohne respond endet await im Timeout.
