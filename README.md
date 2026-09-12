# Phore Message Queue

PhoreMQ ist der Entwurf einer frameworkunabhängigen PHP-Library für **RabbitMQ**.
Die erste Umsetzung enthält genau einen `RabbitMQConnector` hinter
`ConnectorInterface`. Es gibt keine Adapterregistrierung, Brokerauswahl oder
Fallback-Logik. Öffentliche Konfiguration verwendet die generischen Begriffe
Namespace, Topic, Subscription und Nachrichtentyp.

**Status: Die PHP-API ist noch nicht implementiert.** Composer-Metadaten und
Autoloading stammen aus der Projektvorlage; PHP >=8.5 ist verbindlich. Docker Compose und das PHP-
Setup sind unabhängig von der geplanten PHP-Library verwendbar.

Vom Repository-Verzeichnis aus:

```bash
docker compose -f deployment/rabbitmq/compose.yaml up -d --wait
php deployment/rabbitmq/setup.php
```

AMQP: `amqp://demo:demo@127.0.0.1:5672/demo`; Management:
`http://127.0.0.1:15672` mit `demo` / `demo`. Nur lokale Entwicklung.
Die PHP-Beispiele sind weiterhin API-Entwürfe.

- [Architekturentscheidung und API-Proposal](docs/proposals/2026-09-12-message-queue-api.md)
- [Installation, Docker-Start und dynamische Topologie](docs/setup.md)
- [Gemeinsame Konfiguration](config/message-queue.json)
- [Message Queue Basics 101](docs/message-queue-basics-101.md)
- [Verbindung und zentrale Konfiguration](examples/api-draft/01-connect.php)
- [Programmatisch senden und empfangen](examples/api-draft/02-programmatic.php)
- [SDK-Typen und Handler mit Attributen](examples/api-draft/03-attributes.php)
- [ZIP-Dateien per verifizierter Speicherreferenz](examples/api-draft/04-files-and-local.php)
- [RPC mit Ergebnissen, Warnings und Remote-Exceptions](examples/api-draft/05-rpc.php)
- [Metadaten und Middleware](examples/api-draft/06-metadata-middleware.php)
- [Broadcast und Antworten aller erwarteten Teilnehmer](examples/api-draft/07-broadcast-locking.php)
- [Processing-Queue: ein Worker und ein Ergebnis](examples/api-draft/08-processing-workers.php)
- [Systemcheck und Dienststatus](examples/api-draft/09-system-check.php)
- [Callback-Fehler, Retry und Fehlerablage](examples/api-draft/10-callback-errors.php)

Das zentrale Objekt wird einmal pro Prozess mit DSN oder Adapter und
`ConnectionOptions` erzeugt. `ConnectionFactory` bleibt ein einfacher
Konstruktor-Helfer. Alle Beispiele laden dieselbe Konfiguration über
`new PhoreMQ(...demoConnection())`; konkrete Verbindungsoptionen stehen in Beispiel 01.

`publish($dto)` oder `publish($topic, $type, $payload)` sendet sofort.
`subscribe` registriert Events, `respond` Commands, `run` verarbeitet
Zustellungen. `publish(...)->await(timeoutSeconds: 5)` wartet optional auf eine
Antwort und wirft bei Ablauf `RequestTimeoutException`. `await` sendet nichts
noch einmal. Eigene freigegebene Remote-Exceptions sind möglich.

`subscribe($callback)` und `#[Subscribe]` übernehmen die Metadaten des ersten
DTO-Parameters; widersprüchliche Angaben werden früh abgelehnt. Optional prüft
und hydriert `phore/schema` die lokale Struktur. Sender und Empfänger müssen
nicht dieselbe PHP-Klasse verwenden. Metadaten, HMAC-Signierung, Middleware und
verifizierte Dateireferenzen bleiben außerhalb der fachlichen Payload.

**An alle:** eigene Subscription pro Empfänger. **An einen:** mehrere Worker
verwenden dieselbe Subscription. RabbitMQ verteilt Zustellungen; wiederholte
Verarbeitung bleibt möglich und benötigt fachliche Idempotenz.
`check()` prüft Verbindung und nachrichtenspezifische Bereitschaft. `HealthState`
meldet Dienstprobleme und Wiederherstellung im gemeinsamen Statusformat.
Ein Broker-Ping allein beweist keinen bereiten Handler.

## Git Submodules

Beim Klonen direkt mit auschecken:

```bash
git clone --recurse-submodules <repo-url>
```

Nachträglich initialisieren oder aktualisieren:

```bash
git submodule update --init --recursive
git submodule update --remote --merge
```

Projektregel: Ausführbare Beispiele und Setup-Skripte werden ausschließlich in PHP >=8.5 gepflegt; siehe [AGENTS.md](AGENTS.md).
