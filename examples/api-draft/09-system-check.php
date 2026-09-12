<?php

declare(strict_types=1);

namespace Examples\MessageQueue\SystemCheck;

use Phore\MessageQueue\PhoreMQ;
use Phore\MessageQueue\ConnectionOptions;
use Phore\MessageQueue\Exception\RetryableMessageException;
use Phore\MessageQueue\Health\CheckOptions;
use Phore\MessageQueue\Health\HealthFinding;
use Phore\MessageQueue\Health\HealthOptions;
use Phore\MessageQueue\Health\HealthState;
use Phore\MessageQueue\Health\ReadinessRequirement;
use Phore\MessageQueue\MessageQueueInterface;
use Phore\MessageQueue\SubscriptionOptions;

/** API-ENTWURF, keine ausführbare Implementierung. Proposal § 16.
 * Lokaler Einstieg wie in 01-connect.php; hier ausschließlich Health-Konfiguration.
 */

// Beispiel eines definierten Anwendungsfehlers aus dem injizierten Exporter.
final class OutputPermissionException extends \RuntimeException {}

function runExportWorker(
    string $instanceId, string $host,
    string $outputDirectory, callable $processExport,
): void {
    $state = new HealthState(serviceId: 'export-service', instanceId: $instanceId);
    $denied = HealthFinding::unhealthy(
        code: 'OUTPUT_PERMISSION_DENIED',
        publicMessage: 'Der Exportdienst kann sein Ausgabeziel nicht beschreiben.',
        action: 'Berechtigungen des Ausgabeziels prüfen.',
    );
    $refresh = static function (HealthState $state) use ($outputDirectory, $denied): void {
        // Nur eine indikative, lesende Prüfung; ein späterer Schreibzugriff kann scheitern.
        clearstatcache(true, $outputDirectory);
        $state->set('output.writable',
            is_dir($outputDirectory) && is_writable($outputDirectory)
                ? HealthFinding::healthy()
                : $denied,
            topic: 'jobs.export', type: 'export.create.v1',
        );
    };
    $state->set('output.writable', HealthFinding::unknown(
        code: 'DEPENDENCY_UNCHECKED', publicMessage: 'Ausgabeziel noch nicht geprüft.',
        action: 'Nächste Prüfung abwarten.',
    ), topic: 'jobs.export', type: 'export.create.v1');
    $refresh($state);

    $mq = new PhoreMQ('file:///tmp/phore-mq-demo', new ConnectionOptions(health: new HealthOptions(
        state: $state,
        refresh: $refresh,
        refreshIntervalSeconds: 5,
        statusTtlSeconds: 20,
        // Opt-in: Host nur an autorisierte Betreiber, nicht ungefiltert an Browser.
        diagnostics: static fn (): array => [
            'host' => $host,
            'processMemoryBytes' => memory_get_usage(true),
            'processPeakMemoryBytes' => memory_get_peak_usage(true),
        ],
    )));
    try {
        $mq->respond('jobs.export', 'export-workers',
            static function (array $parameters) use ($processExport, $state, $denied): array {
                try {
                    return $processExport($parameters); // Anwendung prüft/idempotent verarbeitet.
                } catch (OutputPermissionException $error) {
                    // Gleicher Zustand für Push, Ping und Consume-Pause; kein Container-Abbruch.
                    $state->set('output.writable', $denied,
                        topic: 'jobs.export', type: 'export.create.v1');
                    throw new RetryableMessageException('Export temporarily unavailable', 0, $error);
                }
            }, new SubscriptionOptions(type: 'export.create.v1'));
        // Loop bedient Health auch während einer Bereitschaftspause und prüft Recovery.
        // Ein beliebig blockierender Handler benötigt ein separates Laufzeitmodell (§ 16.4).
        $mq->run();
    } finally {
        $mq->close();
    }
}

function frontendConnection(string $instanceId): MessageQueueInterface
{
    return new PhoreMQ('file:///tmp/phore-mq-demo', new ConnectionOptions(health: new HealthOptions(
        state: new HealthState(serviceId: 'frontend-backend', instanceId: $instanceId),
        requirements: [
            // Mein System BRAUCHT diesen Nachrichtentyp, mit mindestens einem Worker.
            new ReadinessRequirement(
                topic: 'jobs.export', type: 'export.create.v1',
                subscriptions: ['export-workers'], minReadyPerSubscription: 1,
            ),
            // Broadcast: Jede dieser beiden Gruppen muss wenigstens einmal bereit sein.
            new ReadinessRequirement(
                topic: 'users.events', type: 'user.created.v1',
                subscriptions: ['audit-service', 'mail-service'],
            ),
        ],
    )));
}

function checkAtLogin(MessageQueueInterface $mq): array
{
    $connection = $mq->check(); // Ausschließlich Broker/lokale Konfiguration.
    // Alle deklarierten Anforderungen in EINEM begrenzten Check, pro Ziel mit Listenerliste.
    $system = $mq->check(options: new CheckOptions(requireDeclared: true, timeoutSeconds: 3));
    // Alternativ nur die Exportfunktion; Anforderungen werden aus Konfiguration übernommen:
    $export = $mq->check('jobs.export', 'export.create.v1')->toArray();

    // Beispiel: Nur erlaubte Felder zum Browser. Interne Host-/Prozessdaten bleiben im Backend.
    return ['features' => ['export' => [
        'ready' => $export['ready'],
        'status' => $export['status'],
        'expiresAt' => $export['expiresAt'],
        'issues' => array_map(static fn (array $issue): array => [
            'code' => $issue['code'], 'message' => $issue['publicMessage'],
        ], $export['issues']),
    ]]];
    // $system->toArray()['targets'] enthält zusätzlich jedes benötigte Topic/Typ-Paar.
    // Jeder Zielbericht enthält consumers samt readiness, issues und optional diagnostics.
    // Im echten Loginpfad einen passenden Check wählen; alle drei dienen hier dem API-Vergleich.
}

function observeStatus(MessageQueueInterface $monitor, callable $acceptVerifiedSnapshot): void
{
    $monitor->subscribe('_phore.health.status', 'operations-dashboard',
        static function (array $snapshot) use ($acceptVerifiedSnapshot): void {
            // Collector prüft Identität, generation/sequence und TTL (§ 16.6),
            // speichert aktuellen Zustand und löst deduplizierte Alarme/Recovery aus.
            $acceptVerifiedSnapshot($snapshot);
        }, new SubscriptionOptions(type: 'system.health.v1'));
    $monitor->run(); // Auf separater Connection, nicht im Login-Request.
}
