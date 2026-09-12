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
use Phore\MessageQueue\Rpc\AwaitOptions;
use Phore\MessageQueue\Rpc\CommandFailedException;
use Phore\MessageQueue\Rpc\Notice;
use Phore\MessageQueue\Rpc\RemoteCommandException;
use Phore\MessageQueue\Rpc\RequestContext;
use Phore\MessageQueue\Rpc\RequestTimeoutException;
use Phore\MessageQueue\SubscriptionOptions;

/**
 * API-ENTWURF, noch nicht ausführbar. Proposal §§ 13–14.
 * Nach Implementierung: zuerst runServer() in Prozess A starten,
 * dann divideFromApplication() in Prozess B. Beide nutzen denselben RabbitMQ-Namespace.
 * RPC-Ablauf (geplante Library, nicht manuell in der Anwendung nachzubauen):
 * 1. Vor publish: private exklusive Classic-Reply-Queue + eindeutiges Binding
 *    an gemeinsamer interner Exchange anlegen und Reply-Consumer bestätigen lassen.
 * 2. Neue requestId lokal registrieren, dann Request mit replyTo/requestId senden.
 * 3. Ein Worker verarbeitet; return bzw. sichere Exception wird zum Reply.
 * 4. Reply an genau dieses replyTo senden und bestätigen lassen, dann Request ack.
 * 5. await liest den Rückkanal und ordnet anhand requestId zu; kein Hintergrundthread.
 *
 * Mehrere Publisher: je Connection eigene Queue, mehrere Calls je Queue per ID.
 * Gleicher Nachrichtentyp ist KEIN gemeinsamer Rückkanal. Worker-Replikate teilen
 * calculator-workers; Publisher-Replikate teilen niemals ihre Reply-Queue.
 *
 * Container-Neustart: Der Publisher verliert lokale Pending-Objekte. RabbitMQ
 * löscht die exklusive Queue nach erkanntem Verbindungsverlust (nicht zwingend
 * sofort beim Crash). close() räumt sie auf, Binding wird mit entfernt; die eine
 * gemeinsame interne Exchange bleibt. Neuer Container bekommt einen neuen Namen.
 * Ein einzelner await-Timeout löscht die gemeinsam genutzte Queue NICHT.
 * Nach Wire-Deadline werden Pending-Einträge entfernt, späte Replies verworfen.
 *
 * Worker-Crash vor Request-Ack kann Arbeit erneut zustellen: auch nach return
 * oder nach erfolgreicher Geschäftsaktion! Division ist rein und wiederholbar.
 * Bei Buchungen/Exports operationId und Ergebnis extern dauerhaft/atomar speichern;
 * nie nur einen Array-Cache im Container verwenden. Nach Publisher-Neustart mit
 * derselben operationId bewusst neu anfragen, aber neuer requestId/replyTo.
 * Der Worker sendet das gespeicherte Ergebnis an das AKTUELLE replyTo zurück.
 * Unklare Publish-Bestätigung oder Timeout beweist nicht, dass nichts passiert ist.
 * Ist das alte Reply-Ziel nachweislich gelöscht, keine Geschäftsaktion allein
 * deswegen wiederholen; Ergebnis sichern und verwaisten Request abschließen.
 * Bei unklarem Reply-Publish kein Ack. Details/Fehlerfenster: Proposal § 13.6.
 * Docker: demoConnection nutzt Host-Loopback; in getrennten Containern gemeinsame
 * Konfiguration auf rabbitmq:5672 und http://rabbitmq:15672 umstellen (Setup).
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

// Worker-Entrypoint: erst Handler registrieren, dann blockierend empfangen.
function runServer(): void
{
    $mq = new PhoreMQ(...demoConnection());
    try {
        $mq->respond('calculator', 'calculator-workers', [new DivideHandler(), 'divide'],
            new SubscriptionOptions(type: 'math.divide.v1'));
        // Alternative zur Zeile oben: $mq->registerHandlers(new DivideHandler());
        $mq->run(); // return aus divide() wird zum Reply; danach Request-Ack.
    } finally {
        $mq->close();
    }
}

// HTTP-/Anwendungsschicht: ein Command, ein Ergebnis.
// await wirft RequestTimeoutException (Ausgang unbekannt) oder RemoteCommandException
// (sicherer fachlicher Fehler). Die Anwendung darüber entscheidet über ihre Fehlerantwort.
function divideFromApplication(float $a, float $b): float
{
    $mq = new PhoreMQ(...demoConnection());
    try {
        $reply = $mq->request(new Divide($a, $b))->await(timeoutSeconds: 5);
        return $reply->payload['quotient'];
    } finally {
        $mq->close(); // Diese Funktion besitzt ihre Connection, auch im Fehlerfall.
    }
}

// Erweiterter Aufruf: Wire-Deadline beim Senden, Auswertung erst beim Warten.
function divideWithNotices(): array
{
    $mq = new PhoreMQ(...demoConnection());
    try {
        $sent = $mq->request('calculator', 'math.divide.v1', ['a' => 12, 'b' => 0.5],
            new PublishOptions(
                replyTimeoutSeconds: 15, // Antwortfrist ab Senden; enthält Queue-Wartezeit.
                metadata: ['app.traceId' => 'trace-demo-42', 'app.locale' => 'de-DE'],
            )); // Schon gesendet, kein Builder. request erzwingt den Rückkanal vor Versand.

        $reply = $sent->await(
            timeoutSeconds: 5, // Nur dieser lokale Warteaufruf, längstens bis Wire-Deadline.
            onNotice: static function (Notice $notice): void {
                printf("%s [%s]: %s\n", $notice->level, $notice->code, $notice->message);
            },
        );
        // Warnings sind Zwischenmeldungen, kein Abbruch und keine Fristverlängerung.
        // Ergebnis-Payload, Metadaten und finale Notice-Zusammenfassung bleiben getrennt.
        // onNotice wurde schon ausgeführt; reply->notices nicht nochmals ausgeben.
        return ['quotient' => $reply->payload['quotient'], 'worker' => $reply->metadata['app.worker']];
    } finally {
        $mq->close(); // Exceptions bleiben für die aufrufende Anwendung sichtbar.
    }
}

// Lokale Exception ausdrücklich freigeben; keine entfernte PHP-Deserialisierung.
function divideWithTypedError(float $a, float $b): float
{
    $mq = new PhoreMQ(...demoConnection());
    try {
        $reply = $mq->request(new Divide($a, $b))->await(
            timeoutSeconds: 5, errorTypes: [DivisionByZero::class],
        );
        return $reply->payload['quotient'];
    } catch (DivisionByZero $error) {
        printf("Eingabe korrigieren: %s\n", $error->getMessage());
        throw $error; // Freigegebene SDK-Klasse und sichere Servermeldung.
    } catch (RemoteCommandException $error) {
        throw $error; // Andere Remote-Fehler behalten den generischen Typ.
    } finally {
        $mq->close();
    }
}

// Gleichwertige Sendemethode: publish mit explizit aktivierter Antwortfähigkeit.
function divideUsingPublish(float $a, float $b): float
{
    $mq = new PhoreMQ(...demoConnection());
    try {
        $sent = $mq->publish(new Divide($a, $b), options: new PublishOptions(reply: true));
        return $sent->await(timeoutSeconds: 5)->payload['quotient'];
        // Mit rpc.enabled in der Connection darf reply:true auch entfallen.
        // request($dto) drückt die RPC-Absicht bereits im Methodennamen aus.
        // PublishOptions(reply:false) sendet ein Event; späteres await ist dann ein Fehler.
    } finally {
        $mq->close();
    }
}

// Nach lokalem Timeout denselben Handle weiterverwenden, solange Wire-Frist läuft.
function divideWithSecondWait(float $a, float $b): float
{
    $mq = new PhoreMQ(...demoConnection());
    try {
        $sent = $mq->request(new Divide($a, $b), options: new PublishOptions(replyTimeoutSeconds: 15));
        try {
            $reply = $sent->await(timeoutSeconds: 2);
        } catch (RequestTimeoutException) {
            $reply = $sent->await(timeoutSeconds: 5); // Kein zweiter Request.
        }
        return $reply->payload['quotient'];
    } finally {
        $mq->close();
    }
}

// Ergebnis-Darstellung gehört zum lokalen Empfänger, unabhängig von der Serverklasse.
final class LocalDivisionResult
{
    public float $quotient;
}

function divideAsLocalDto(float $a, float $b): LocalDivisionResult
{
    $mq = new PhoreMQ(...demoConnection());
    try {
        $reply = $mq->request(new Divide($a, $b))->await(
            timeoutSeconds: 5, responseClass: LocalDivisionResult::class,
        );
        return $reply->payload; // Struktur prüfen/hydrieren; keine Server-FQCN vergleichen.
    } finally {
        $mq->close();
    }
}

// Alternativ await(new AwaitOptions(timeoutSeconds: 5)); direkte Werte überschreiben
// dasselbe Feld. responseClass gehört nur zu await und hydriert das lokale Ergebnis.
// await prüft Optionen NACH dem Senden: falsche Klasse/ungültiger Timeout kann den
// bereits veröffentlichten Request nicht zurücknehmen. DTOs brauchen die Schema-Bridge.
// ConnectionException/PublishException entstehen beim Transport und werden nicht als
// RemoteCommandException umgedeutet. Bei Abbruch können alte Handles nicht auf eine
// neue Connection übertragen werden. Die Anwendung entscheidet über Wiederanlauf.
