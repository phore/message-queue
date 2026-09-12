# Phore Message Queue

Geplante universelle PHP-Library für Message Queues mit austauschbaren
Konnektoren. Redis Streams bildet die erste Implementierung; eine gemeinsame
API verbindet Topics, dauerhafte Subscriptions und konkurrierende Worker.
Optionale strukturelle Validierung und DTO-Hydration verwenden `phore/schema`
aus dem Repository `phore/phore-schema`. Nachrichtentypen und Handler lassen
sich programmatisch oder über PHP-Attribute zuordnen. Eine austauschbare
Sicherheitsschicht signiert Nachrichten transparent mit HMAC-SHA-256;
große Dateien werden über verifizierte Speicherreferenzen transportiert.
Der Entwurf umfasst außerdem Request/Reply für RPC, getrennte Metadaten
und zwei optionale Middleware-Hooks für Senden und Handler-Ausführung.

**Status: API-Entwurf, noch keine Queue-Implementierung.** Composer-Metadaten
und Autoloading stammen weiterhin aus der Projektvorlage. Die folgenden
PHP-Dateien zeigen die vorgeschlagene API und sind noch nicht ausführbar.

- [API-Entwurf, Konnektorvergleich und Paketgrenzen](docs/proposals/2026-09-12-message-queue-api.md)
- [Verbinden: URL, Konnektor und Attribute](examples/api-draft/01-connect.php)
- [Programmatisch senden und empfangen](examples/api-draft/02-programmatic.php)
- [SDK-Typen und Handler mit Attributen](examples/api-draft/03-attributes.php)
- [ZIP-Dateien per Speicherreferenz](examples/api-draft/04-files-and-local.php)
- [RPC: Command, Ergebnis, Warnings und Fehler](examples/api-draft/05-rpc.php)
- [Metadaten und Diagnose-Middleware](examples/api-draft/06-metadata-middleware.php)
- [Broadcast und Antworten aller Lock-Teilnehmer](examples/api-draft/07-broadcast-locking.php)
- [Processing-Queue: ein verfügbarer Worker und ein Ergebnis](examples/api-draft/08-processing-workers.php)
- [Systemcheck, Dienststatus und frühzeitige Fehlermeldungen](examples/api-draft/09-system-check.php)

**An alle:** pro Empfänger eine eigene Subscription. **An einen:** alle Worker
verwenden dieselbe Subscription. Das Backend verteilt die Zustellungen an
verfügbare Worker; gleichmäßiger Zufall oder Exactly-once-Ausführung werden
nicht vorausgesetzt.

Die Alltags-API bleibt klein: `publish()` sendet ein Event, `subscribe()`
empfängt Events, `publish(...)->await()` wartet optional auf eine Antwort, `respond()`
registriert einen Command-Handler und `run()` verarbeitet Nachrichten.
`publish($dto)` übernimmt Topic und Typ aus den Metadaten des Objekts;
eine separate `emit`-Methode ist nicht mehr vorgesehen.
Für Diagnose ergänzt `check()` einen standardisierten Bericht über Verbindung
und Consumer-Bereitschaft. Dienste können über einen gemeinsamen `HealthState`
Probleme aktiv melden und betroffene Verarbeitung pausieren; Frontend und
Monitoring nutzen dasselbe Statusformat.
Der [Frameworkvergleich und die API-Entscheidung in § 14.1](docs/proposals/2026-09-12-message-queue-api.md)
begründen diesen Ansatz.

Das zentrale Objekt ist `Phore\MessageQueue\PhoreMQ`:
`$mq = new PhoreMQ($dsn, $options)` oder `new PhoreMQ($connector, $options)`.
Die optionalen `ConnectionOptions` bündeln die gesamte weitere Konfiguration.
Die `ConnectionFactory` liefert ebenfalls `PhoreMQ`, das
`MessageQueueInterface` implementiert. Einmal je Verbindung erzeugen,
wiederverwenden und mit `close()` freigeben; beide Wege verbinden sofort.

`subscribe($callback)` übernimmt Topic, Subscription und Wire-Typ aus dem
Mapping des ersten DTO-Parameters; am Handler reicht alternativ `#[Subscribe]`.
Offene Werte werden explizit ergänzt, widersprüchliche feste Angaben und
doppelte lokale Bindungen schon beim Registrieren abgelehnt. Für mehrere
Topics bleibt das Topic am Contract offen; feste Subscriptions bedeuten
konkurrierende Worker. Siehe [Attributbeispiele](examples/api-draft/03-attributes.php).

`publish` sendet sofort und liefert `SendResult` mit Broker-Beleg `receipt`.
Mit konfiguriertem RPC-Rückkanal wartet optional
`publish($command)->await(timeoutSeconds: 5)` auf ein Responder-Ergebnis;
`await` sendet nicht erneut. Ohne Rückkanal ist späteres Warten nicht möglich.
Häufige Optionen gehen direkt: `run(maxMessages: 100, maxSeconds: 30)`;
`RunOptions`/`AwaitOptions` bleiben erlaubt, direkte Werte überschreiben ihre
entsprechenden Felder. Das vollständige Beispiel steht in
[05-rpc.php](examples/api-draft/05-rpc.php).

Einfacher lokaler Einstieg im Entwurf: `new PhoreMQ('file:///tmp/phore-mq-demo')`.
Die Voraussetzungen dieses geplanten Entwicklungsadapters stehen zentral in
[01-connect.php](examples/api-draft/01-connect.php); Redis bleibt Produktionsstandard.
Callback-Fehler und begrenzte Retries zeigt
[10-callback-errors.php](examples/api-draft/10-callback-errors.php).

[Message Queue Basics 101](docs/message-queue-basics-101.md) erklärt Begriffe,
Zustellung, Aufbewahrung und die Grenzen der geplanten Adapter.

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

Deklarierte Nachrichtenabhängigkeiten lassen sich gemeinsam mit
`check(options: new CheckOptions(requireDeclared: true))` prüfen. Der Bericht
zeigt Listener, Bereitschaft, Ursachen und optional Host-/Speicherdiagnose;
fehlende Antworten gelten als unbekannt statt als sicher fehlender Listener.
