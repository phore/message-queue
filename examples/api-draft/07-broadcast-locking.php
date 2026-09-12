<?php

declare(strict_types=1);

namespace Examples\MessageQueue\BroadcastLocking;

use Phore\MessageQueue\PhoreMQ;
use Phore\MessageQueue\Exception\RejectMessageException;
use Phore\MessageQueue\MessageContext;
use Phore\MessageQueue\PublishOptions;
use Phore\MessageQueue\RunOptions;
use Phore\MessageQueue\SubscriptionOptions;

/**
 * API-ENTWURF, noch nicht ausführbar. Proposal § 15.2.
 * Zuerst zwei Prozesse mit runParticipant(..., 'service-a', $localLocksA)
 * und runParticipant(..., 'service-b', $localLocksB) starten/provisionieren.
 * Danach EINEN Koordinator mit withAllLocks(..., ['service-a', 'service-b'], $work).
 * Jede Instanz, die antworten soll, braucht eine eigene Subscription/Teilnehmer-ID.
 * Kooperative Entwicklungsakteure: Produktions-Identitätsprüfung siehe Proposal.
 */

// Anwendungsschnittstelle für echte lokale Leases, KEIN neuer Teil der MQ-API.
// Die Implementierung wird von der Anwendung injiziert und läuft unabhängig
// vom Queue-Worker ab. Das Beispiel implementiert keinen verteilten Lockstore.
interface LocalLeaseManager
{
    // Idempotent je Ressource/Runde; kein Stehlen anderer Locks, keine Verlängerung
    // bei Redelivery. Nach Ablauf oder Abschlussmarke derselben Runde immer false.
    public function tryAcquire(string $resource, string $roundId, int $leaseUntilUnix): bool;

    // Nur eigene Runde freigeben; Abschlussmarke bis leaseUntilUnix aufbewahren,
    // auch wenn ein Release vor dem zugehörigen Acquire eintrifft.
    public function release(string $resource, string $roundId, int $leaseUntilUnix): void;
}

function runParticipant(string $participantId, LocalLeaseManager $locks): void
{
    $mq = new PhoreMQ('file:///tmp/phore-mq-demo');
    try {
        // ENTSCHEIDEND: Jede erwartete Instanz hat einen ANDEREN Subscription-Namen.
        $mq->subscribe('maintenance.locks', 'locks-' . $participantId,
            static function (array $command, MessageContext $context) use ($mq, $participantId, $locks): void {
                if (!in_array($context->type, ['lock.acquire.v1', 'lock.release.v1'], true)) {
                    return;
                }
                if (!is_string($command['roundId'] ?? null)
                    || ($command['resource'] ?? null) !== 'search-index'
                    || !is_array($command['participants'] ?? null)
                    || !is_int($command['acquireBy'] ?? null)
                    || !is_int($command['leaseUntil'] ?? null)
                    || $command['leaseUntil'] <= $command['acquireBy']
                    || $command['leaseUntil'] > time() + 30
                    || $context->correlationId !== $command['roundId']) {
                    throw new RejectMessageException('Ungültige Lock-Koordinationsnachricht.');
                }
                if (!in_array($participantId, $command['participants'], true)) {
                    return; // Neue Teilnehmer gehören erst zum nächsten Snapshot.
                }

                if ($context->type === 'lock.release.v1') {
                    $locks->release($command['resource'], $command['roundId'], $command['leaseUntil']);
                    return;
                }

                $held = time() < $command['acquireBy'] && $locks->tryAcquire(
                    $command['resource'], $command['roundId'], $command['leaseUntil'],
                );
                // Fester Rückkanal; keine URL/DSN aus der Nachricht verwenden.
                $mq->publish('maintenance.replies.coordinator-demo', 'lock.state.v1', [
                    'roundId' => $command['roundId'],
                    'participant' => $participantId,
                    'held' => $held,
                    'leaseUntil' => $command['leaseUntil'],
                ], new PublishOptions(correlationId: $command['roundId']));
                // Erst erfolgreiche Rückkehr bestätigt Acquire; bei Retry bleibt
                // tryAcquire mit derselben Runde idempotent.
            });
        $mq->run();
    } finally {
        $mq->close(); // Die injizierte Lease-Verwaltung sichert den Ablauf selbst.
    }
}

/**
 * @param list<string> $participants Fester Membership-Snapshot, keine Subscriber-Zählung.
 * @param callable(string, int): void $criticalSection Runde und sichere Ablaufzeit.
 * Die Anwendung muss Ablauf/Fencing AN DER ZIELRESSOURCE durchsetzen; ein
 * PHP-Zeitvergleich allein schützt nicht vor Prozesspausen/Lease-Verlust.
 */
function withAllLocks(array $participants, callable $criticalSection): void
{
    foreach ($participants as $participant) {
        if (!is_string($participant) || $participant === '') {
            throw new \InvalidArgumentException('Teilnehmer-IDs müssen nicht leere Strings sein.');
        }
    }
    if ($participants === [] || count(array_unique($participants)) !== count($participants)) {
        throw new \InvalidArgumentException('Eine eindeutige, nicht leere Teilnehmerliste ist erforderlich.');
    }

    $roundId = bin2hex(random_bytes(16)); // Korrelations-/Ownership-ID, KEIN Fencing-Token.
    $acquireBy = time() + 5;
    $leaseUntil = $acquireBy + 10;
    $safeUntil = $leaseUntil - 2; // Nur Demo-Reserve; Produktion benötigt begründete Grenzen.
    $states = [];
    $rejected = false;
    $command = [
        'roundId' => $roundId,
        'resource' => 'search-index',
        'participants' => $participants,
        'acquireBy' => $acquireBy,
        'leaseUntil' => $leaseUntil,
    ];

    $mq = new PhoreMQ('file:///tmp/phore-mq-demo');
    try {
        // Vor dem Broadcast binden, damit auch sofortige Antworten erfasst werden.
        $mq->subscribe('maintenance.replies.coordinator-demo', 'lock-coordinator',
            static function (array $state, MessageContext $context) use (
                $mq, $roundId, $participants, $leaseUntil, &$states, &$rejected,
            ): void {
                if (($state['roundId'] ?? null) !== $roundId || $context->correlationId !== $roundId) {
                    return; // Alte/fremde Runde, nicht mitzählen.
                }
                $participant = $state['participant'] ?? null;
                if (!is_string($participant) || !in_array($participant, $participants, true)
                    || !is_bool($state['held'] ?? null) || ($state['leaseUntil'] ?? null) !== $leaseUntil) {
                    throw new RejectMessageException('Ungültige Lock-Bestätigung.');
                }
                // In Produktion hier zusätzlich verifizierte Teilnehmeridentität
                // bzw. pro Teilnehmer geschützten Rückkanal prüfen, nicht nur den Text.
                $states[$participant] = $state['held']; // Duplikate zählen nicht doppelt.
                if (!$state['held']) {
                    $rejected = true;
                }
                if ($rejected || count($states) === count($participants)) {
                    $mq->stop();
                }
            }, new SubscriptionOptions(type: 'lock.state.v1'));

        // EIN Publish erreicht ALLE benannten Teilnehmer-Subscriptions.
        $mq->publish('maintenance.locks', 'lock.acquire.v1', $command,
            new PublishOptions(correlationId: $roundId));
        $remaining = $acquireBy - time();
        if ($remaining > 0) {
            // Nur das verbleibende Zeitbudget dieser Runde; kein neuer voller Timeout.
            // run kehrt bei Ablauf normal zurück. Ob alle geantwortet haben, prüfen wir
            // unten selbst; maxSeconds garantiert weder Teilnehmerzahl noch Lock-Erfolg.
            $mq->run(new RunOptions(maxSeconds: $remaining));
        }

        if ($rejected || count($states) !== count($participants)
            || time() >= $acquireBy || time() >= $safeUntil) {
            throw new \RuntimeException('Nicht alle Teilnehmer haben ihre Lease rechtzeitig bestätigt.');
        }

        // Alle bekannten Teilnehmer haben geantwortet. Das ist eine Barriere,
        // kein eigenständiger globaler Lock und keine unbegrenzte Gültigkeit.
        $criticalSection($roundId, $safeUntil);
    } finally {
        try {
            // Auch bei negativer Antwort/Timeout teilweise erworbene Locks freigeben.
            $mq->publish('maintenance.locks', 'lock.release.v1', $command,
                new PublishOptions(correlationId: $roundId));
        } catch (\Throwable) {
            // Keine falsche Freigabegarantie: Leases müssen unabhängig ablaufen.
            error_log('Lock-Release nicht bestätigt; automatische Lease-Abläufe bleiben erforderlich.');
        } finally {
            $mq->close();
        }
    }
}
