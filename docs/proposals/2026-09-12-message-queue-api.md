# Phore Message Queue: API- und Architekturentwurf

| Datum | Benutzername | Kurzbeschreibung |
|---|---|---|
| 2026-09-12 | dermatthes | §§ 1–12: Proposal mit API-Beispielen, Konnektorvergleich und Paketgrenzen angelegt |
| 2026-09-12 | dermatthes | §§ 1, 3, 5, 8, 11–14: Kleine explizite API, RPC, Begleitmeldungen, Metadaten und Middleware nach Frameworkvergleich ergänzt |
| 2026-09-12 | dermatthes | §§ 2, 12, 13.2, 15: Broadcast mit allen Lock-Antworten und Processing-Queue mit konkurrierenden Workern ergänzt |
| 2026-09-12 | dermatthes | §§ 1.1, 16: Standardisierte Systemchecks, deklarierte Nachrichtenabhängigkeiten, Listenerdiagnose und Frontend-/Monitoring-Anbindung ergänzt |
| 2026-09-12 | dermatthes | §§ 1, 1.1, 3, 4: PhoreMQ als zentrales Objekt mit DSN-/Connector-Konstruktor und gleichwertiger Factory-Erzeugung ergänzt |
| 2026-09-12 | dermatthes | §§ 5, 6, 6.2, 11: Callback-Kurzform, abgeleitete Metadaten, offene Topics und frühe Konfliktprüfung ergänzt |
| 2026-09-12 | dermatthes | §§ 1.1, 2, 5, 6.2, 11, 13, 13.1, 13.2, 13.4, 14.1: Einheitliches publish für DTO/Explizitform, optionales await, direkte Laufzeitparameter und Deadline-Regeln ergänzt |
| 2026-09-12 | dermatthes | §§ 5, 13.4: Worker-Limits, fehlendes minMessages und await-Timeout-Exception in den Beispielen erläutert |
| 2026-09-12 | dermatthes | §§ 4, 4.1, 7, 8, 10, 11.1, 13.1, 13.2, 13.4, 13.5: Kurze lokale file-Beispiele, zentrale Verbindungsvorgaben, Callback-Fehler und typisierte RPC-Exceptions ergänzt |
| 2026-09-12 | dermatthes | §§ 1–5, 7–16: RabbitMQ als einzige Umsetzung beschlossen; Interface ohne Austauschlogik, neutrale Konfiguration, Docker-Setup und Beispiele vereinheitlicht |
| 2026-09-12 | dermatthes | §§ 1, 10: PHP 8.5 als Mindestversion und ausschließlich PHP-Beispiele/Setup festgelegt |
| 2026-09-12 | dermatthes | §§ 2, 7, 11: Listener provisionieren; Publisher werfen bei fehlender Topologie QueueConfigurationMissingException |

## § 1 Abstract und Lieferumfang

**Architekturentscheidung, 2026-09-12: Die erste Umsetzung verwendet ausschließlich RabbitMQ über AMQP 0-9-1.** Ein `RabbitMQConnector` implementiert `ConnectorInterface`; die MQ-Logik spricht nur dieses Interface an. Es gibt keine Adapterregistrierung, Treiberauswahl, Capability-Aushandlung, Fallbacks oder Laufzeit-Austauschlogik. Die Interface-Grenze ermöglicht spätere Änderungen, ohne heute zusätzliche Broker zu entwerfen.

Die frameworkunabhängige PHP-Library bietet Topics, dauerhafte Subscriptions,
konkurrierende Worker, optionale strukturelle `phore/schema`-Hydration und
PHP-Attribute. `PhoreMQ` akzeptiert DSN oder Adapter mit `ConnectionOptions`.
RPC, Fehlerantworten, Metadaten, Middleware und Systemcheck bleiben Bestandteil
des Entwurfs. Signierung und Dateireferenzen behalten ihre fachlichen Verträge.

**Die PHP-API ist noch nicht implementiert.** Composer-Metadaten und Autoloading
stammen aus der Vorlage; die PHP-Mindestversion ist verbindlich >=8.5.
Der Docker-Start und das PHP-Setup aus [Setup](../setup.md) sind davon
unabhängige Entwicklungsdateien; sie implementieren keine MQ-Library. Alle
ausführbaren Beispiele und Setup-Skripte dieses Projekts sind in PHP >=8.5
zu schreiben; verbindliche Projektregeln stehen in [AGENTS.md](../../AGENTS.md).

| Umfang | Entscheidung |
|---|---|
| Transport | Genau ein RabbitMQ-Adapter hinter `ConnectorInterface` |
| Zustellung | Dauerhafte Subscriptions, Quorum Queues, Publisher Confirms, Ack nach Handler-Erfolg |
| API | `publish`, `subscribe`, `respond`, `run`, optional `await`; `check` für Diagnose |
| Konfiguration | Generische Namen; `topic`, `subscription`, `type`, `namespace`, `maxInFlight`, `autoCreate` |
| Entwicklung | Derselbe RabbitMQ-Adapter gegen einen Docker-Broker |
| Dateiübertragung | Verifizierte Referenzen über ausdrücklich injizierten Dateispeicher; keine eigene Speicherplattform |

Die Referenz für den Entwicklungsaufbau ist RabbitMQ 4.3 mit Management-Plugin.
Die produktive PHP-Client-Abhängigkeit wird bei Implementierung festgelegt.

### § 1.1 Kleine API auf einen Blick

Die Empfehlung ist die konkrete Queue-Fassade `PhoreMQ`, die
`MessageQueueInterface` implementiert, mit den Operationen `publish`, `subscribe`, `respond` und `run`.
Senden, Warten und Empfang sind am jeweiligen Aufruf erkennbar; Broker, Routing,
Schema und Middleware werden einmal am Objekt konfiguriert.
`publish` sendet sofort und liefern ein `SendResult`; nur dessen
optional aufgerufenes `await` wartet auf eine fachliche Antwort.

```php
$mq = new PhoreMQ($dsn, $options); // Einmal erzeugen; DSN oder Connector.
$mq->publish('users', 'user.created.v1', ['userId' => 'u-1']);
$mq->subscribe('users', 'billing-users', function (array $event): void { /* ... */ });

$reply = $mq->publish('calculator', 'math.divide.v1', ['a' => 12, 'b' => 3])
    ->await(timeoutSeconds: 5); // Rückkanal einmal in ConnectionOptions konfigurieren.
echo $reply->payload['quotient']; // 4; wartet ausdrücklich auf eine entfernte Antwort.

$mq->respond('calculator', 'calculator-workers', function (array $params): array {
    return ['quotient' => $params['a'] / $params['b']]; // Kurzform; vollständige Fehlerprüfung in Beispiel 05.
}, new SubscriptionOptions(type: 'math.divide.v1'));
$mq->run();
```

Diese Zeilen illustrieren getrennte Sender-/Empfängerprozesse, kein sequenziell
ausführbares Skript; der Responder muss vor dem Request laufen. Konstruktor
bzw. Factory und `close()` gehören zum Verbindungslebenszyklus. `publish($dto)` ist
die Objektform derselben Sendemethode mit automatischem Mapping; Attribute registrieren dieselben
Handler. Es gibt keine zweite RPC-Client-Fassade, kein eigenes Promise-Framework,
keinen Container-Zwang und kein mehrdeutiges `dispatch(..., true)`. Erweiterungen
kommen über Optionsobjekte und zwei Middleware-Hooks; Signierung, Codec und
Konnektoren sind Infrastruktur-Schnittstellen, keine Pflicht im täglichen Code.
`request` bleibt eine optionale explizite RPC-Komfortform, ist für das Warten
nach `publish` aber nicht mehr erforderlich.

Für Diagnose gibt es zusätzlich genau einen Queue-Aufruf `check()`.
Dienstentwickler melden Zustandsänderungen über `HealthState::set()` in einem
gemeinsamen lokalen Zustandsobjekt; Transport, aktive Meldung und Ping-Antwort
verwaltet die Library. Details und standardisierter Vertrag in § 16.

## § 2 Begriffe und Zustellvertrag

| Begriff | Bedeutung und Beispiel |
|---|---|
| Topic | Logischer Nachrichtenkanal, etwa `users`; unabhängig vom Backendnamen |
| Message type / Subject | Fachlicher Vertrag und Routingbezeichnung, etwa `user.created.v1`; API-Name ist ausschließlich `type`, kein zusätzliches Subject-Argument |
| Namespace | Isolierter Namensraum aus dem DSN-Pfad, etwa `demo`; im Adapter auf einen Virtual Host abgebildet |
| Subscription | Dauerhafte benannte Sicht auf ein Topic, etwa `billing-users` |
| Worker | Ein Prozess, der für eine Subscription arbeitet; mehrere teilen sich die Arbeit |
| Envelope | Transportneutrale Metadaten, JSON-Payload und Attachment-Deskriptoren |
| Delivery | Eine konkrete Zustellung inklusive opaque Receipt und Ack-/Retry-Steuerung |

Jede dauerhafte Subscription erhält eine Kopie. Worker derselben Subscription
sind konkurrierende Consumer. Beispiel: `billing-users` und `audit-users`
erhalten beide `user.created.v1`; drei Billing-Worker teilen sich die
Billing-Zustellungen. Innerhalb eines Workers erhält ein Nachrichtenvertrag
genau einen registrierten Handler je Subscription; doppelte Registrierung
ist ein Konfigurationsfehler. Weitere unabhängige Handler verwenden eigene
Subscriptions. Ein Worker kann mehrere Topics abonnieren. „An alle“ bezeichnet
alle passenden benannten Subscriptions; „an einen“ einen ausgewählten Worker
innerhalb derselben Subscription. Die Subscription-Topologie bestimmt das
Verhalten, kein zusätzlicher Broadcast-Schalter beim Senden. Beispiele in § 15.

Der Grundvertrag lautet **at least once innerhalb der konfigurierten
Aufbewahrung und Verfügbarkeit**. Doppelte Zustellungen sind möglich, ebenso
eine unklare Publish-Bestätigung bei Verbindungsabbruch.
`SendResult::receipt` enthält ein `PublishReceipt`: Es bestätigt Backend-Annahme,
keine Verarbeitung durch Empfänger. `SendResult::await()` liefert dagegen die
fachliche Antwort eines Responders, keine Bestätigung aller Subscriber. Es gibt keine
Exactly-once-Garantie und keine globale Reihenfolge.
Fachliche Seiteneffekte benötigen eine stabile fachliche Idempotenz-ID; Wiederzustellungen behalten zusätzlich dieselbe `messageId`.

`subscribe()` bindet eine benannte Subscription und prüft ihren Vertrag.
Eine neu angelegte Subscription empfängt erst Nachrichten ab Erstellung ihrer
Bindung. Ein bestehender Rückstand bleibt bei Worker-Neustarts erhalten.
Es gibt weder Start-Cursor noch Replay-Option. In Produktion werden fachliche
Topics und Subscriptions vorab eingerichtet. `autoCreate: true` erlaubt
explizit ihre dynamische Anlage beim Registrieren von Listenern; `cancel()` beendet nur den lokalen Consumer,
löscht aber weder Subscription noch Rückstand.

## § 3 Abstraktionsschichten und Erweiterungspunkte

| Baustein | Verantwortung |
|---|---|
| `ConnectionFactory` / `ConnectionOptions` | Alternative Erzeugung von `PhoreMQ` und gemeinsame Konfiguration; derselbe RabbitMQ-Verbindungsaufbau wie im Konstruktor |
| `PhoreMQ` | Zentrales Objekt; akzeptiert DSN oder Connector und Optionen, implementiert `MessageQueueInterface` und verwaltet den Lebenszyklus |
| `MessageQueueInterface` | `publish`, `subscribe`, `request`, `respond`, `run`; Mapping-Komfort und Lebenszyklus gemäß § 1.1 |
| `MessageRegistry` | Fachliche Namen, Sendeklassen, optionale Schemas und Default-Topics zuordnen |
| `MessageCodecInterface` | JSON-kompatible Daten normalisieren, Envelope serialisieren und dekodieren |
| `SchemaMapperInterface` | Optional Strukturen prüfen und in lokal konfigurierte DTOs hydrieren |
| `MessageSecurityInterface` | Unveränderliche Nachrichtenbytes schützen und vor Verwendung verifizieren |
| `ConnectorInterface` | RabbitMQ kapseln: Topologie prüfen/anlegen, Bytes senden/empfangen und Zustellungen abschließen |
| `PayloadStoreInterface` | Streams ablegen, Referenzen auflösen, Lebensdauer verwalten |
| `RetryPolicy` / `FailureStoreInterface` | Vorübergehende Fehler wiederholen, endgültige Fehler sicher ablegen |

Sendepfad: Typ/Topic auflösen → Send-Middleware ausführen → Daten normalisieren
und ggf. validieren → Dateien ablegen → Envelope kodieren → signieren → Größenprüfung
→ Konnektor. Empfangspfad: begrenzten Transportframe lesen → Signatur und
Zeit-/Zielbindung prüfen → Envelope dekodieren → optional Dateien verifizieren
→ lokale Struktur prüfen/hydrieren → Handler-Middleware und Handler ausführen
→ bei RPC finale Antwort bestätigen lassen → Ack. Dateiinhalte werden erst
bei Zugriff geladen, bleiben aber vor Nutzung zu prüfen. Middleware darf weder
die Signaturprüfung noch Settlement umgehen; Details in § 14.3.

Der Konnektor kennt keine Anwendungs-DTOnamen oder Callbacks. Seine
vorgeschlagenen primitiven Operationen sind `ensureTopology(TopologyDefinition, bool $autoCreate): void`,
`publish(OutboundFrame): TransportReceipt`, `receive(ReceiveRequest): iterable`,
`ack(DeliveryToken): void`, `release(DeliveryToken, RetryOptions): void` und
`close(): void`. `receive` respektiert Timeout/Stop und liefert `InboundFrame`
mit Routingkontext und Receipt; nackte Receipts gelangen nie in Nachrichten.
Ungültige oder bereits erledigte Receipts erzeugen eine Settlement-Exception.

Ein `MessageSecurityInterface` bietet `protect(string $envelopeBytes,
SecurityContext $context): ProtectedFrame` und `verify(ProtectedFrame $frame,
SecurityContext $context): string`. Fehler werfen Exceptions. Der Kontext
enthält das lokal erwartete Topic und die Audience. Alle Verbindungswege,
einschließlich direkter Konnektoren, durchlaufen dieselbe Sicherheitskette.
Ein Provider kann auch verschlüsseln; HMAC allein tut dies nicht.

## § 4 Verbinden und DSN-Factory

[01-connect.php](../../examples/api-draft/01-connect.php) zeigt die Varianten.
Der Konstruktor verbindet sofort, einmal pro Prozess; `close()` gibt Ressourcen
idempotent frei. `stop()` beendet nur den Worker-Loop. Teilweise geöffnete
Ressourcen werden bei Fehlern geschlossen. Ein Adapter gehört exklusiv einem MQ.

```php
$mq = new PhoreMQ($dsn, $options);
$mq = new PhoreMQ(new RabbitMQConnector($dsn), $options);
$mq = (new ConnectionFactory())->connect($dsn, $options);
```

Diese Zeilen sind Alternativen. Die Factory ist ein einfacher Konstruktor-Helfer;
keine Provider-Registry, keine Verbindungsattribute und keine dynamische Auswahl.
`PhoreMQ implements MessageQueueInterface` hat weiterhin den Konstruktor
`__construct(string|ConnectorInterface $connection, ?ConnectionOptions $options = null)`.
Ein String wird ausschließlich als RabbitMQ-AMQP-Verbindung ausgewertet.
Direkte Injektion bleibt für die Interface-Grenze und Tests erhalten; sie umgeht
weder Codec, Security noch Middleware. Weitere Implementierungen werden nicht geliefert.

| DSN | Bedeutung |
|---|---|
| `amqp://demo:demo@127.0.0.1:5672/demo` | Lokaler Broker, Namespace `demo` |
| `amqps://user:password@mq.example.org:5671/app` | TLS mit Zertifikats-/Hostprüfung, Namespace `app` |

DSN-Bestandteile werden einmal percent-dekodiert; `@` im Passwort ist `%40`,
der Namespace `/` wird als `/%2F` dargestellt. Ungültige Ports, Schemes,
Query-Optionen oder Pfade werden abgelehnt. Fehlerausgaben redigieren Credentials.
Kein Environment-Zugriff und keine automatische Secret-Erzeugung. Die
Security-Policy muss explizit vorliegen; eine reine DSN ohne erforderliche
Optionen schlägt früh mit `InvalidConfigurationException` fehl.

`ConnectionOptions` bündelt `autoCreate` (Standard false), den expliziten
`managementUrl` für Topologieprüfungen, `maxInFlight`
(Standard 1, positive Ganzzahl), Security, Registry, optionale Schema-Bridge,
RPC, Health, Middleware und optionalen PayloadStore. Konfiguration wird als
Snapshot übernommen; bewusst geteilte Zustandsobjekte wie `HealthState`
bleiben geteilt. Unbekannte Optionen sind Fehler.

`ConnectionOptions::fromArray(array $values, ?ConnectionOptions $overrides = null)`
ist ein geplanter Konfigurationshelfer, keine existierende Implementierung.
Er versteht ausschließlich dokumentierte Werte: `security.mode=unsigned`
wählt explizit die unsignierte Demo-Policy, `rpc.enabled` und `rpc.replyNamespace`
den Rückkanalmodus aus § 13.1. Keine Klassennamen oder ausführbarer Code aus JSON.
Explizit gesetzte Override-Felder ersetzen die entsprechenden Basiswerte;
ausgelassene Felder behalten sie. Objekt-Abhängigkeiten werden nur programmatisch
injiziert. Die optionale Schema-Bridge wird bei installiertem `phore/schema`
verwendet; andernfalls scheitert benötigte DTO-Hydration früh.

### § 4.1 Kurzer lokaler Einstieg in den Beispielen

[config/message-queue.json](../../config/message-queue.json) ist die gemeinsame
Quelle für Verbindung, Optionswerte und deklarierte fachliche Topologie.
[connection.php](../../examples/api-draft/connection.php) lädt diese Datei
explizit. Die Beispiele erzeugen weiterhin ein einzelnes `PhoreMQ`:

```php
$mq = new PhoreMQ(...demoConnection());
```

`demoConnection()` ist nur eine Beispiel-Hilfsfunktion, kein neuer Library-Aufruf.
Sie liefert DSN und Optionen; spezifische Optionen können ergänzt werden.
Die Demo ist ausdrücklich unsigniert und nur für den isolierten lokalen Broker.
Für produktive Dienste werden eigene Credentials, TLS und die HMAC-Policy
konfiguriert. Broker-Passwort und Signierschlüssel sind verschiedene Werte.
Die Demo richtet pro Client einen eigenen Rückkanal ein. Es gibt keinen
impliziten Dateispeicher; Beispiel 04 verlangt ihn ausdrücklich vom Aufrufer.

Docker-Start, Einrichtung, Namensabbildung, dynamische Anlage und Bereinigung
sind im [Setup-Guide](../setup.md) beschrieben. Bestehende Subscriptions behalten
ihren Backlog; unabhängige Beispieldurchläufe beginnen mit einem frischen Demo-Broker.

## § 5 Senden, empfangen und Worker-Lebenszyklus

[Programmatische Beispiele: 02-programmatic.php](../../examples/api-draft/02-programmatic.php).
Die vorgeschlagenen öffentlichen Signaturen lauten:

```php
publish(string|object $topic, string|PublishOptions|null $type = null,
    array|object|null $payload = null, ?PublishOptions $options = null): SendResult;
subscribe(string|callable $topic, ?string $subscription = null,
    ?callable $handler = null, ?SubscriptionOptions $options = null): SubscriptionHandle;
request(string $topic, string $type, array|object $params,
    ?RequestOptions $options = null): SendResult;
respond(string $topic, string $subscription, callable $handler,
    ?SubscriptionOptions $options = null): SubscriptionHandle;
registerHandlers(object $handler): void;
run(?RunOptions $options = null, ?int $maxMessages = null,
    ?float $maxSeconds = null, ?float $idleTimeoutSeconds = null): void;
stop(): void;
close(): void;
```

`publish($topic, $type, $payload)` benennt Topic und Typ ausdrücklich;
`publish($dto)` liest sie aus Registry oder Attribut der lokalen Sendeklasse. Ein fehlendes oder widersprüchliches
Mapping wirft `MessageMappingException`. Registry, Attribute und explizite
Angaben ergänzen nur offene Werte; widersprüchliche feste Angaben werden
abgelehnt statt still überschrieben. Mehrfache programmatische Registrierung
desselben Sendetyps wird abgelehnt. `PublishOptions` kann eine stabile `messageId`, `expiresAt`,
`correlationId`, getrennte `metadata` und Attachments tragen. Ein Retry eines
unklar bestätigten Publishes verwendet dieselbe ID und denselben fachlichen
Inhalt. `request`/`respond` sind die optionale RPC-Erweiterung aus § 13;
`subscribe` sendet niemals automatisch einen Rückgabewert.

`publish` entscheidet ausschließlich anhand des ersten Parameters: Ein String
ist das explizite Topic und benötigt einen String-Typ sowie Array-/Objekt-Payload.
Ein Objekt ist die gesamte Payload; Topic und Wire-Typ werden aus seinem lokalen
Mapping ergänzt. Für diese Form sind `publish($dto, $options)` und
`publish($dto, options: $options)` gleichwertig. Der Parametername `topic` bleibt
für bestehende benannte Aufrufe erhalten. Ein Array als erster Parameter wird
nicht als DTO interpretiert. Ohne Mapping kein Ableiten aus dem PHP-Klassennamen.

`PublishOptions` kann `topic` und `type` für offene Mappingwerte enthalten,
etwa `publish($dto, options: new PublishOptions(topic: 'audit.users'))`.
Identische Angaben sind zulässig, widersprüchliche feste Angaben aus Klasse,
Registry oder explizitem Aufruf werfen weiterhin `MessageMappingException`.
Zusätzliche Payload/Typ-Argumente in der Objektform, ein Optionsobjekt an
zweiter und vierter Position oder eine unvollständige explizite Form werden
vor Publish als `InvalidArgumentException` abgelehnt. Diese Fälle werden nicht
heuristisch umgedeutet. Die bisher vorgeschlagene Methode `emit` entfällt,
auch als Alias, da die API noch nicht implementiert ist.

`SubscriptionOptions` enthält optional `topic` und `subscription` für die
Callback-Kurzform sowie `type` als exakten Filter,
`payloadClass` als lokale Zielklasse, `ackMode` und `retryPolicy`.
Fachliche Subscriptions sind immer dauerhaft; es gibt keine Cursor-, Replay-
oder wechselbaren Haltbarkeitsmodi.
Ohne Typfilter muss der Array-Handler alle
Nachrichtentypen des Topics verarbeiten können. Das Binding filtert bereits bei der Zustellung in die Queue. Ein dennoch
eingehender unpassender Frame ist ein Routing-/Validierungsfehler und wird
sicher abgelegt; kein separater Handler darf dieselbe Subscription mit anderem Filter übernehmen.
Filteränderungen benötigen eine neue Subscription oder explizite Migration.

Die bisherigen Aufrufe `subscribe($topic, $subscription, $handler, $options)`
bleiben gültig. Neu ist `subscribe($callback)` bzw.
`subscribe($callback, options: new SubscriptionOptions(...))`. Der erste
Parameter heißt zur Kompatibilität weiter `topic`; ist `handler` gesetzt,
muss `topic` ein String sein (explizite Form). Ohne `handler` muss er ein
aufrufbarer Callback sein (Kurzform). Ein String ohne Handler wird nur als
existierender Funktionsname akzeptiert, niemals als automatisch entdeckter
Topic-Handler. Eine Subscription in der zweiten Position ergänzt bei der
Kurzform einen offenen Wert. Vermischte ungültige Aufrufe werden mit
`InvalidHandlerException` abgelehnt. Ein wirklich leeres `subscribe()` besitzt
keinen Callback und ist kein gültiger Aufruf.

`subscribe` registriert und bindet, `run` startet den blockierenden Empfang.
Vorgesehen: `RunOptions(maxMessages, maxSeconds, idleTimeoutSeconds)` oder
direkt `run(maxMessages: 100, maxSeconds: 30, idleTimeoutSeconds: 2)`;
`run` kehrt beim ersten erreichten Limit zurück. `stop` beendet nach dem
laufenden Handler, `close` gibt Verbindungen frei. Empfangs-Timeout ohne
Nachricht ist kein Fehler. Ein Handler erhält Payload und optional
`MessageContext`; letzterer liefert `messageId`, Typ, Topic, Versuch und
Attachment-Zugriff. Broker-spezifische Objekte werden nicht weitergereicht.

Optionsobjekte bleiben als erster Parameter erlaubt, etwa
`run(new RunOptions(maxSeconds: 60), maxMessages: 100)`. Nicht-null direkt
angegebene Parameter überschreiben denselben Wert aus dem Optionsobjekt;
nicht angegebene Werte übernehmen dessen Konfiguration bzw. Library-Defaults.
Das Optionsobjekt wird nicht mutiert. Limits müssen endlich und positiv sein,
`maxMessages` ganzzahlig; ungültige Werte werfen `InvalidArgumentException`
vor Eintritt in den Loop. Diese Regel gilt auch für die direkten Parameter
von `await`; Routingkonflikte aus § 6.2 werden dagegen weiterhin abgelehnt.
`run` bedient alle registrierten Handler, nie das vorherige Sendekommando.

`maxMessages` zählt abgeschlossene fachliche Zustellversuche über alle
Subscriptions dieses `run` zusammen; der Zähler startet je Aufruf bei null.
Fan-out-Kopien und Wiederholungen zählen separat, auch Versuche mit behandeltem
Retry-/Reject-/Validierungsfehler. Interne Health-/Reply-Verarbeitung, leere
Polls und vorgeholte, noch nicht bearbeitete Zustellungen zählen nicht.
Ein wegen ungültigem Typ abgelehnter Delivery zählt als abgearbeiteter Versuch. Das Limit ist keine Anzahl erfolgreicher oder
eindeutiger Geschäftsoperationen. Nach Erreichen wird keine weitere fachliche
Zustellung verarbeitet; bereits vorgeholte Einträge bleiben sicher unbestätigt
bzw. werden beim Schließen des Empfangschannels erneut verfügbar.

`maxSeconds` ist das Gesamtbudget ab Loop-Start, einschließlich Warten und
Verarbeitung. `idleTimeoutSeconds` begrenzt eine zusammenhängende Wartephase
ohne fachliche Zustellung; bei Rückkehr ins Warten beginnt diese Frist neu,
Handlerlaufzeit zählt nicht als Leerlauf. Interner Health-Verkehr setzt den
Idle-Timer nicht zurück. Empfangswarten wird auf die verbleibenden Budgets
begrenzt. Ein laufender synchroner Handler wird nicht präemptiv abgebrochen
und kann das Gesamtbudget überschreiten; weitere Handler starten dann nicht.
Erreichen eines Worker-Limits führt zu normaler Rückkehr, nicht zu einer
Timeout-Exception. Echte Infrastrukturfehler bleiben Exceptions.

`minMessages` gehört nicht zum Entwurf. `maxMessages` ist eine Obergrenze,
keine Mindestzahl: Zeitlimit, Leerlauf oder `stop()` können den Loop früher
beenden. Nicht gesetzte Grenzen sind unbegrenzt; `run()` ohne Limits läuft
bis Stop oder Fehler. Eine fachlich erforderliche Mindestzahl bestätigt die
Anwendung explizit, wie die Lock-Antwortaggregation in Beispiel 07. Die
Optionskommentare stehen ausführlich in Beispiel 02 und am RPC-Loop in 05.

Standardmäßig folgt Ack erst nach erfolgreicher Callback-Rückkehr. Ein
temporärer Handlerfehler löst eine begrenzte Retry-Policy aus; endgültige
Fehler gehen vor Bestätigung in den FailureStore. `AckMode::Manual` erlaubt
`context->ack()`, `context->retry(delaySeconds: ...)` oder
`context->reject(reason: ...)`. Es ist genau eine Settlement-Entscheidung pro
Zustellung zulässig. Rückkehr ohne Settlement gibt die Nachricht erneut frei.
Eine Lease-Verlängerungsmethode gehört nicht zur API. Lange synchrone Handler
brauchen passende Broker-Ack-Fristen und eine Laufzeit, die AMQP-Heartbeats
bedient; eine offene TCP-Verbindung allein verhindert keinen Heartbeat-Abbruch.

Retry erfolgt durch RabbitMQ-Redelivery und die in § 7 beschriebene Adapterlogik.
Es darf keine verlustbehaftete Folge aus Ack vor erneutem Publish geben.
Nichtatomare Kopier-vor-Ack-Schritte dürfen Duplikate erzeugen und müssen
dies dokumentieren. Fehler im FailureStore führen zu **keinem Ack** und
beenden den Worker mit Infrastrukturfehler. Ein Error-Observer bekommt
sanitisierte Fehlerdaten; systemische Transportfehler werden aus `run`
geworfen statt in einer Endlosschleife verborgen.

## § 6 SDK-Typen, Attribute und strukturelle Kompatibilität

[Attributbeispiele: 03-attributes.php](../../examples/api-draft/03-attributes.php).
Mit „Annotationen“ sind zunächst native PHP-8-Attribute gemeint:
`#[MessageType('user.created.v1', topic: 'users', subscription: 'sdk-users')]`
auf DTOs ermöglicht `subscribe($callback)` mit einem typisierten ersten
Parameter. Alternativ reicht `#[Subscribe]` auf einer öffentlichen
Handler-Methode; `registerHandlers($object)` verwendet denselben Resolver.
Offene Werte werden am Handler oder Aufruf ergänzt. PHPDoc-Annotationen als
zweites Metadatensystem sind vorerst nicht vorgesehen; die normalen
PHPDoc-Feldtypen von `phore/schema` bleiben nutzbar.

Ein SDK enthält ausschließlich Contracts/DTOs und optionale Attribute, keine
Connection, Secrets oder Worker. Ein SDK darf auch ganz ohne MQ-Attribute
auskommen: `registry->register('user.created.v1', T_UserCreated::class,
topic: 'users')` ordnet bestehende Klassen von außen zu. Ein Empfänger darf
eine eigene Klasse `LocalUserCreated` benutzen. Der Wire-Typname stimmt
überein; weder Composer-Paketname noch PHP-FQCN werden verglichen oder als
Class-Loading-Anweisung übertragen. Gleiche Kurzklassennamen allein erzeugen
kein Mapping; der fachliche Name muss lokal bewusst zugeordnet sein.

| Callback-Parameter | Geplantes Verhalten |
|---|---|
| `array $data` | Dekodierte Daten; keine DTO-Hydration; Schema-Prüfung nur bei explizit registriertem Contract |
| `LocalDto $data` | Reflection bestimmt die lokale Klasse; Schema-Bridge validiert/hydriert vor dem Aufruf |
| Explizites `payloadClass` | Muss zum Callback passen; Konflikt wird schon beim Registrieren abgelehnt |
| Untyped / `mixed` | Array wie bei untypisiertem Empfang |
| Union, Intersection, Interface, abstrakte Klasse | Keine automatische Zielwahl; im ersten Release `InvalidHandlerException` |

Typisierte Handler benötigen einen eindeutigen Message-Typ aus
`SubscriptionOptions::type`, `Subscribe::type` oder Klassenmapping. Ohne ihn
ist die Registrierung ungültig. Die optionale Schema-Bridge muss verfügbar
sein, sobald DTO-Hydration oder Contract-Validierung verlangt wird;
sonst `MissingDependencyException`, niemals stiller Rückfall auf Arrays.
Der Resolver aus § 6.2 prüft alle vorhandenen Angaben auf Übereinstimmung.

Kompatibilität bedeutet: Pflichtfelder müssen vorhanden sein, die vorhandenen
bekannten Felder müssen rekursiv ihren Datentypen entsprechen. Zusätzliche
Felder sind im vorgeschlagenen `Compatible`-Profil erlaubt und werden vor
Hydration auf die bekannte lokale Struktur projiziert. Der untypisierte
Empfang bewahrt sie. `Strict` lehnt unbekannte Felder ausdrücklich ab.
Defaults und nullable Felder folgen zunächst `phore/schema`: ohne Default
und ohne Nullbarkeit ist ein Feld required; nullable darf auch fehlen.
„Required und nullable“ braucht später eine eigene explizite Regel.
Inkompatible Änderungen bekommen einen neuen Wire-Namen, etwa `.v2`.

### § 6.1 Tatsächliche Integration in phore/schema

Geprüfte Einstiegspunkte sind `SchemaParser::parseClass`,
`Hydrator::hydrate`, `ClassSchema::hydrate` und `Validator::validate`.
Composer nennt das Paket `phore/schema`; das Repository heißt
`phore/phore-schema`. Der Hydrator akzeptiert Array-/stdClass-Strukturen und
kann verschachtelte Klassen daraus erzeugen, lehnt aber unbekannte Properties
ab. Der aktuelle Validator verwendet bei `ClassReferenceSchemaType` teilweise
`instanceof`; bloß `validate($schema, $wireArray)` erfüllt deshalb den hier
verlangten rekursiven Strukturvertrag nicht zuverlässig.

Die geplante `PhoreSchemaMapper`-Bridge normalisiert Objekte anhand öffentlicher
Schema-Properties in Wire-Strukturen, projiziert im Compatible-Profil rekursiv
auf bekannte Felder und verwendet den Hydrator als strukturelle Prüfung und
DTO-Erzeugung. Class-References werden nur über lokal konfigurierte Schemas
aufgelöst. Cycles, maximale Tiefe und Größen werden begrenzt; Maps und Listen
bleiben unterscheidbar. Untypisierte Daten werden ohne lokale DTO-Klasse
dekodiert; reine Contract-Prüfung darf intern hydrieren und das Ergebnis
verwerfen. Konstruktoren solcher Contracts müssen seiteneffektfrei sein.

Ein allgemeiner `Validator::validate()`-Aufruf wird im Entwurf daher nicht
als fertige Wire-Validierung dargestellt. Ein verbesserter struktureller
Validator in `phore/schema` wäre eine gesonderte spätere Aufgabe. Klassen-
Identitätsprüfungen für Wire-Payloads sowie `unserialize()` sind ausgeschlossen.
JSON unterstützt nur definierte Datentypen; Ressourcen, Closures, Zyklen,
NaN/Infinity und unbekannte Objekttypen werfen `SerializationException`.

### § 6.2 Metadaten einmal definieren und aus dem Callback ableiten

```php
#[MessageType('user.created.v1', topic: 'users', subscription: 'sdk-users')]
final class UserCreated { public string $userId; }

$mq->subscribe(function (UserCreated $event): void {
    // Topic users, Subscription sdk-users, Typ user.created.v1;
    // Payload wird vor dem Callback strukturell geprüft und hydriert.
});
```

`MessageType(string $type, ?string $topic = null, ?string $subscription = null)`
bezeichnet einen stabilen Wire-Typ und optional feste Routingwerte.
`Subscribe(?string $topic = null, ?string $subscription = null, ?string $type = null)`
kann ohne Argumente auf einer Methode stehen. Reflection untersucht den ersten
Payload-Parameter des Callbacks; der optionale zweite `MessageContext` liefert
keine Routingwerte. Closures, Funktionsnamen, öffentliche Methoden-Callables
und aufrufbare Objekte werden einheitlich über ihre tatsächliche Signatur
aufgelöst, ohne den Callback auszuführen. Mehrdeutige Payload-Typen bleiben
wie in § 6 beschrieben ungültig.

Der Resolver sammelt Klassenmapping/`MessageType`, ein gegebenenfalls am
Callback vorhandenes `Subscribe`-Attribut und explizite Aufruf-/Optionswerte.
Für Topic, Subscription, Typ und Zielklasse gilt: fehlend lässt sich ergänzen;
mehrere identische Angaben sind zulässig; unterschiedliche feste Angaben
sind ein Fehler. Es gibt keinen stillen Vorrang. Methodennamen, PHP-FQCNs,
Hostname oder Instanz-ID werden niemals zu Topic- oder Subscriptionnamen
umgedeutet. Für SDKs ohne Attribute kann
`registry->register($type, $class, topic: ..., subscription: ...)` dieselben
Metadaten lokal hinterlegen.

Topic und Subscription müssen nach Auflösung eindeutig vorhanden sein;
für einen DTO-Handler zusätzlich der Wire-Typ. Untypisierte/Array-Handler
benötigen explizite Topic-/Subscription-Angaben und optional den Typfilter.
Ohne Typfilter bleibt ihr bisheriger Empfang aller Typen des Topics gültig.
`payloadClass` muss weiterhin zum Callback passen. Ohne Schema-Bridge gibt
es auch in der Kurzform keine automatische DTO-Hydration.

Ein für mehrere Topics verwendeter Contract lässt `MessageType::topic`
vollständig weg. Das Topic wird für jede Registrierung explizit gewählt;
es gibt keine automatische Expansion, kein Wildcard-Abonnement und keine
Liste im `topic`-Feld. Für unabhängige Gruppen bleibt entsprechend
`MessageType::subscription` offen. Der fachliche Typ wird weiterhin aus der
Klasse übernommen:

```php
#[MessageType('audit.entry.v1')]
final class AuditEntry { public string $text; }

$handler = function (AuditEntry $event): void { /* ... */ };
$mq->subscribe($handler, options: new SubscriptionOptions(
    topic: 'audit.users', subscription: 'audit-reader',
));
$mq->subscribe($handler, options: new SubscriptionOptions(
    topic: 'audit.billing', subscription: 'audit-reader',
));
```

Die Subscription ist die Gruppe für konkurrierende Worker, kein intrinsischer
Bestandteil der Nachricht auf dem Wire. Eine im SDK festgelegte Subscription
wird daher nur gewählt, wenn diese Gruppierung absichtlich für alle Nutzer
gelten soll. Mehrere Dienste, die dieselben vollständigen Metadaten übernehmen,
konkurrieren um Arbeit; für Fan-out braucht jeder seine eigene, am Contract
offen gelassene Subscription. Ein gleicher Gruppenname an unterschiedlichen
logischen Topics bezeichnet unterschiedliche Bindungen.

`publish($dto)` verwendet dieselben festen Topic-/Typ-Angaben; die Subscription
spielt beim Senden keine Rolle. Ohne festes Topic ergänzt die Anwendung es mit
`publish($dto, options: new PublishOptions(topic: ...))` oder verwendet
`publish($topic, $type, $dto)` bzw. ein Array. Bei einem gemappten DTO müssen
explizite Topic-/Typ-Werte zu dessen festen Metadaten passen; ein offenes
Topic lässt sich frei ergänzen. `publish` ohne auflösbares Topic wirft
`MessageMappingException`. Mehrere Topics werden durch mehrere ausdrückliche
Publishes angesprochen, nicht durch einen verborgenen Broadcast.

Die Registrierung prüft alle Metadaten vor dem Anlegen/Binden von Ressourcen.
Widersprüche werfen `MessageMappingException` mit `code=MAPPING_CONFLICT`,
`field`, `sources` und den betroffenen nicht geheimen Mappingwerten; fehlende
Pflichtwerte `MAPPING_INCOMPLETE` mit `missingFields`. Ein zweiter lokaler
Handler für dieselbe Topic-/Subscription-Bindung wirft
`InvalidHandlerException` mit `code=DUPLICATE_SUBSCRIPTION`, auch wenn beide
Callbacks gleich aussehen oder verschiedene Typfilter wünschen. Andere
Prozesse derselben Gruppe sind ausdrücklich erlaubt.

`registerHandlers` prüft sämtliche ausgewählten Methoden zunächst gemeinsam
auf lokale Konflikte und bindet danach. Kein globales Dateisystem-Scanning;
eine bereits einzeln registrierte Methode darf nicht durch anschließendes
`registerHandlers` doppelt gebunden werden. Schlägt das tatsächliche Binden
am Broker teilweise fehl, werden die in diesem Aufruf neu geöffneten lokalen
Bindings geschlossen; zuvor bestehende Registrierungen bleiben erhalten.
Dabei bereits angelegte dauerhafte Brokerressourcen werden nicht automatisch
gelöscht. Die Meldung benennt die betroffenen Bindings.

Vorgesehene Contract-Tests: identisches Routing aller drei Registrierungswege,
DTO-Hydration, offene Topics, feste Topic-/Typ-/Subscription-Konflikte, fehlende
Metadaten, doppelte lokale Gruppen, gemeinsame Gruppe in zwei Prozessen,
keine Ressourcenerzeugung bei Metadatenfehlern und Cleanup bei Bindefehlern.
Beispiel 03 zeigt die erfolgreichen Varianten und erwartete Exceptions;
es bleibt ausschließlich API-Entwurf.

## § 7 RabbitMQ-Adapter und Konfigurationsabbildung

Die öffentliche API verwendet generische Begriffe. RabbitMQ-Begriffe erscheinen
nur im Adapter, Deployment und zur Erklärung der konkreten Abbildung.

| Öffentlicher Begriff | RabbitMQ-Abbildung im ersten Adapter |
|---|---|
| Namespace | Virtual Host aus dem DSN-Pfad |
| Topic `users` | Dauerhafte Topic-Exchange `phore.topic:users` |
| Typ / Subject `user.created.v1` | Exakter Routing Key; keine eigene Ressource |
| Subscription `audit-users` | Dauerhafte Quorum Queue `phore.sub:users:audit-users` |
| Subscription ohne Typfilter | Binding mit `#`; empfängt alle Typen dieses Topics |
| Subscription mit `type` | Binding mit genau diesem Typ; keine öffentliche Wildcard-Sprache |
| `maxInFlight` | Consumer-Prefetch; keine Zahl parallel ausgeführter PHP-Callbacks |
| Fehlerablage | Quorum Queue `phore.failure:users:audit-users` je Subscription |

Namen bestehen aus einem führenden Buchstaben/Unterstrich und höchstens 99
weiteren Buchstaben, Ziffern, Unterstrichen, Punkten oder Bindestrichen.
Doppelpunkte in physischen Namen sind dadurch eindeutige Trenner. Reservierte
interne Namen beginnen mit `_phore`; Anwendungstopologien verwenden sie nicht.

`publish` verwendet persistente Frames, Publisher Confirms und `mandatory`.
Eine nicht routbare fachliche Nachricht wirft `QueueConfigurationMissingException`; eine positive
Publish-Bestätigung beweist keine Handler-Bereitschaft und nicht die Existenz
aller fachlich erwarteten Subscriptions. Fachliche Bindungen müssen vor Publish
existieren. Eine Exchange selbst speichert keinen Backlog.

`publish` und `request` legen niemals fachliche Topics, Queues oder Bindings
an, auch nicht bei `autoCreate: true`. Die Anlage gehört dem Listener beim
Registrieren mit `subscribe`/`respond` oder dem expliziten Setup. Der Publisher
meldet ein nachweislich fehlendes Topic oder eine nicht routbare Nachricht mit
`QueueConfigurationMissingException extends InvalidConfigurationException`.
Diese besitzt `reason`, `topic` und `messageType`; erlaubte Gründe sind
`TOPIC_MISSING` und `NO_MATCHING_SUBSCRIPTION`. Beispielmeldung:
„Für Topic users und Typ user.created.v1 fehlt die Queue-Konfiguration.
Möglicherweise wurde der zuständige Listener-Dienst noch nicht initialisiert.“
Keine Credentials oder Payloads in dieser Diagnose. [neu]

Der Fehler entsteht direkt bei `publish`/`request`, nicht erst bei `await`.
Eine Vorabprüfung ersetzt weder `mandatory` noch Publisher Confirms: Wird die
Topologie zwischen Prüfung und Versand entfernt, wird auch die Brokerantwort
in denselben Fehlervertrag übersetzt. Rechtefehler, Verbindungsabbrüche und
unklare Publish-Bestätigungen behalten ihre eigenen Exceptions; sie beweisen
keine fehlende Konfiguration und erlauben keinen blinden automatischen Retry.
Die internen, ausdrücklich aktivierten RPC-Rückkanäle aus § 13.1 bleiben davon
getrennt; sie erzeugen keine fachliche Empfänger-Subscription. [neu]

Eine vorhandene passende dauerhafte Subscription ohne aktiven Worker ist
weiterhin ein gültiges Versandziel und sammelt Backlog. Die Exception sagt
nichts Sicheres über einen fehlenden Container aus. Für aktuelle Listener-
Bereitschaft und alle erwarteten Empfänger dient `check()`; ein routbares
Publish allein beweist nur mindestens ein passendes Ziel. [neu]

`subscribe` validiert/anlegt Exchange, Queue und Binding gemäß `autoCreate`.
Identische Definitionen sind wiederholbar. Abweichende Typfilter, Queue-Eigenschaften
oder Namensbindungen werfen `TopologyConflictException`; kein automatisches
Löschen oder Umbauen gefüllter Queues. `autoCreate: false` prüft nur.
Eine passive AMQP-Queue-Prüfung beweist nicht die vollständige Binding-Konfiguration;
strikte Prüfung nutzt die über `managementUrl` konfigurierte Management-API
mit passenden Rechten. Deren Zugang verwendet die expliziten Verbindungscredentials;
produktive Endpunkte müssen HTTPS mit Zertifikatsprüfung verwenden. Keine
ableitende URL-Heuristik und kein Fallback nach fehlgeschlagener Prüfung. Ohne Prüfmöglichkeit folgt
`TopologyVerificationException`, keine Behauptung erfolgreicher Vollprüfung.

Retry-Veröffentlichungen gehen ausschließlich über ein internes Ziel zurück
an dieselbe Subscription, niemals erneut über die fachliche Topic-Exchange.
Verzögerung erfolgt mit internen Wartequeues fester TTL und Rückführung an die
Zielqueue. Erst nach bestätigtem Retry-/Fehler-Publish wird das Original bestätigt.
Crash-Fenster dürfen Duplikate, aber kein vorzeitiges Erfolgs-Ack erzeugen.
Der unveränderte signierte fachliche Frame bleibt erhalten; Versuchszähler
liegen in vertrauenswürdig verwalteten Transportmetadaten (§ 11.1).

Quorum Queues erhalten bei der Einrichtung bestätigtes Dead-Lettering mit
`reject-publish` und einem eigenen Fehlerziel. Der automatische Delivery-Limit-
Default wird explizit deaktiviert; die begrenzte Handler-Retry-Policy verwaltet
PhoreMQ. Transportabbrüche können zusätzliche Zustellversuche auslösen und
werden nicht als exakte Zahl bereits gestarteter Handler interpretiert.
Die Fehlerqueue erhält keine automatische Ablaufzeit. Betriebsseitige Limits
werden bewusst gesetzt und überwacht; die Demo ist kein Hochverfügbarkeitscluster.

### § 7.1 Was andere PHP-Abstraktionen bereits vorsehen [gelöscht]

## § 8 Transparente Sicherheit

Default-Provider bei konfiguriertem Shared Secret ist **HMAC-SHA-256** mit
Key-ID. Ein bloßer SHA-Hash mit angehängtem Secret ist kein geeignetes
Signaturverfahren. Verbindungskonfiguration verlangt eine explizite Policy:
HMAC oder bewusstes `UnsignedSecurity` für isolierte Tests (§ 4.1). Kein pro Prozess
neu erzeugtes Wegwerf-Secret, fest eingebauter Schlüssel oder stillschweigend
fehlendes Secret. Ein Empfänger mit
HMAC-Policy weist unsignierte Nachrichten immer zurück.

Das signierte Envelope enthält Protokollversion, `messageId`, fachlichen Typ,
Topic, Audience, UTC-`issuedAt`, optional `expiresAt`, Content-Type, Payload,
Correlation-Metadaten und Attachment-Deskriptoren inklusive Digest/Länge.
Anwendungsmetadaten und RPC-Felder (`requestId`, `replyTo`, Deadline,
Nachrichtenart, Notice-ID) sind ebenfalls Teil der geschützten Bytes.
Algorithmus und Key-ID sind ebenfalls kryptografisch gebunden. Ein versionierter
ProtectedFrame transportiert die **exakten ursprünglichen Envelope-Bytes**
und die Signatur; eine längenpräfixierte Signiereingabe mit Domain-Separator
verhindert mehrdeutige Konkatenation. Empfänger serialisieren zur Prüfung
nicht neu. Das Frame-Format muss vor Implementierung mit gemeinsamen
Testvektoren fixiert werden.

Vor Hydration, Callback oder externem Dateiabruf wird mit lokal erlaubtem
Algorithmus/Key geprüft, konstantzeitlich verglichen und das Topic/Audience
mit dem erwarteten Routingkontext abgeglichen. Key-ID dient nur zur Auswahl
aus einem lokalen Keyring. Rotation erlaubt einen aktiven Signierschlüssel
und mehrere verifizierende Schlüssel; keine Algorithmuswahl allein aus
unverifizierten Headern. Der HMAC-Inhaber kann auch selbst signieren:
Shared Secret ist keine individuelle Absenderidentität.

Zeitprüfung toleriert begrenzte Uhrabweichung und lehnt zukünftige oder
abgelaufene Nachrichten ab. Ein optionales maximales Alter muss zum gesamten
Queue-Backlog und Retry-Fenster passen; kein pauschales Fünf-Minuten-
Limit für dauerhafte Queues. Redelivery behält ID, Bytes und ursprüngliche
Signatur. Broker-Versuchszähler gehören nicht zum unveränderlichen Envelope.

Signierung verhindert Replay allein nicht. Eine optionale Inbox speichert
`(audience, subscription, messageId)` mit Zuständen processing/completed und
begrenzten Leases. Erst erfolgreicher Abschluss markiert completed;
fehlgeschlagene Versuche dürfen erneut verarbeitet werden. Ein früher globaler
Nonce-Verbrauch würde legitime Wiederholungen und andere Subscriptions
blockieren. Atomizität zwischen fachlicher DB-Änderung und Inbox erfordert
Anwendungs-/Transaktionsintegration, nicht nur eine transportseitige Duplikaterkennung.

Ungültige Signaturen werden ohne Callback quarantänisiert oder nach expliziter
Policy verworfen, niemals endlos wiederholt. Quarantäne speichert begrenzte
Originalframes und sanitisierte Gründe, keine Secrets. Später kann ein
PGP-Provider dieselbe Schnittstelle implementieren; Keyring, Trust-Modell,
Widerruf und optional Verschlüsselung gehören zu diesem Provider. TLS,
Broker-ACLs und sicherer Dateispeicher bleiben zusätzlich erforderlich.

## § 9 ZIP-Dateien und große Payloads

[Dateibeispiel: 04-files-and-local.php](../../examples/api-draft/04-files-and-local.php).
Die Anwendung übergibt `Attachment::fromPath(...)` oder einen Stream;
die Queue verschickt einen verifizierbaren Deskriptor. Ein konfigurierter
`PayloadStoreInterface` übernimmt Upload und spätere Auflösung. Binärdaten werden nicht unbeschränkt base64-kodiert in die Queue geschrieben.

Der Deskriptor enthält einen opaken Store-Key, Größe, SHA-256, MIME-Typ,
Dateiname und Lebensdauer. Er ist Teil der Signatur; der Digest allein ist
keine Authentifizierung. Der Empfänger lädt ausschließlich über konfigurierte
Store-Adapter, niemals von beliebigen URLs aus Nachrichten. Größe und Digest
werden beim Streamen geprüft. `context->attachment('archive')->copyTo($path)`
schreibt zunächst begrenzt in eine temporäre Datei und veröffentlicht das
Ziel erst nach erfolgreicher Verifikation. ZIP wird nicht automatisch entpackt;
Zielpfad und Entpackungsgrenzen bestimmt die Anwendung.

Upload geschieht vor Publish. Bei unbekanntem Publish-Ergebnis darf die Datei
nicht sofort gelöscht werden. Store-TTL/Lifecycle muss Queue-Retention,
Offline-Consumer, Retry und Dead-Letter-Aufbewahrung plus Reserve abdecken.
Fan-out verbietet Löschung nach dem ersten Ack; initial wird konservative
Lifecycle-Bereinigung statt verteilter Referenzzählung vorgeschlagen.
Verwaiste Uploads werden nach einer Sicherheitsfrist bereinigt. Ein zu früh
abgelaufenes oder fehlendes Objekt erzeugt `AttachmentUnavailableException`.

Der injizierte Dateispeicher muss für Sender und Empfänger erreichbar sein;
RabbitMQ speichert ausschließlich die Referenz, keine automatisch verwaltete ZIP-Datei. Begrenzungen gelten
für Dateigröße, Zahl der Attachments, Downloads und temporären Speicher.
Automatisches Offloading beliebig großer JSON-Bodies sowie Chunking mit
Reassembly sind spätere Erweiterungen und kein impliziter Bestandteil von
`publish`. Ohne Store oder bei zu großem Frame folgt eine eindeutige Exception.

## § 10 Lokale Entwicklung mit RabbitMQ

Die Entwicklung verwendet denselben Adapter wie der spätere Betrieb.
[compose.yaml](../../deployment/rabbitmq/compose.yaml) startet einen einzelnen
RabbitMQ-Knoten mit Management-Plugin und Demo-Namespace. Ports sind nur an
Loopback gebunden. Das benannte Volume überlebt Neustarts; `down -v` entfernt
gezielt den temporären Demo-Zustand. Ein einzelner Quorum-Knoten besitzt keine
Ausfallredundanz. Anleitung und ausführbare Befehle: [Setup](../setup.md).

[setup.php](../../deployment/rabbitmq/setup.php) übersetzt die neutrale
Konfigurationsdatei in RabbitMQ-Deklarationen über dessen HTTP-Management-API.
Es benötigt PHP >=8.5 CLI mit `allow_url_fopen=1`, keine installierte PhoreMQ-Library. `--dry-run` prüft und zeigt die
Operationen ohne Verbindung. Das Skript legt nichts durch Publish an und führt
keine Handler aus. Es löscht keine Ressourcen; entfernte Konfigurationseinträge
entfernen daher keine existierenden Queues. Migrationen sind explizite Vorgänge.

## § 11 Exceptions und Diagnose

Alle Library-Exceptions implementieren ein gemeinsames
`MessageQueueException`-Markerinterface und erben von passenden PHP-
Standardexceptions. Kontext: Fehlercode, Phase, redigierter Konnektorname,
Topic/Subscription, verifizierte `messageId`, optional Feldpfad und
`previous`. Unverifizierte Metadaten sind als solche markiert. Kein kompletter
Payload, Secret, signierter Download-Link oder Receipt im normalen Fehlertext.

| Exception | Beispiel / Behandlung |
|---|---|
| `InvalidDsnException` | Ungültiger Port oder unbekannte Option; Konfiguration korrigieren |
| `MissingDependencyException` | RabbitMQ-Client oder benötigte Schema-Bridge fehlt; vor Workerstart abbrechen |
| `TopologyConflictException` / `TopologyVerificationException` | Deklaration widerspricht bestehender Topologie oder kann nicht vollständig geprüft werden |
| `QueueConfigurationMissingException` | Fachliches Topic fehlt oder keine passende Subscription; Grund `TOPIC_MISSING` oder `NO_MATCHING_SUBSCRIPTION` |
| `ConnectionException` / `AuthenticationException` | Netzwerkproblem retrybar; falsche Credentials nicht endlos wiederholen |
| `PublishException` | Annahme fehlgeschlagen oder unbekannt; `outcome` = rejected/unknown |
| `ReplyNotEnabledException` | await auf ohne Rückkanal gesendeter Nachricht; kein nachträgliches Senden |
| `PendingCapacityExceededException` | Lokale Kapazität für weitere antwortfähige Sends erschöpft; vor Publish ablehnen |
| `MessageMappingException` / `InvalidHandlerException` | `MAPPING_INCOMPLETE`, `MAPPING_CONFLICT`, `DUPLICATE_SUBSCRIPTION` oder mehrdeutige Reflection; Details in § 6.2 |
| `SerializationException` / `InvalidEnvelopeException` | Nicht unterstützte Payload oder defekter Frame; endgültig |
| `MessageValidationException` | `user.created.v1: $.email: required property is missing` |
| `MessageHydrationException` | Konstruktor-/Property-Zuweisung gescheitert; Schema-Exception als previous |
| `InvalidSignatureException` / `ExpiredMessageException` | Keine Verarbeitung; Security-Failure-Policy |
| `PayloadTooLargeException` / `PayloadStoreRequiredException` | Brokergrenze überschritten oder Dateispeicher fehlt |
| `AttachmentUnavailableException` / `AttachmentIntegrityException` | Storefehler ggf. retrybar; falscher Digest endgültig |
| `RetryableMessageException` / `RejectMessageException` | Explizite fachliche Wiederholung bzw. endgültige Ablehnung |
| `SettlementException` | Ack fehlgeschlagen oder Delivery-Channel geschlossen; Duplikate berücksichtigen |
| `FailureStoreException` | Sichere Fehlerablage fehlgeschlagen; kein Ack, Worker abbrechen |

Validierung meldet konkrete Pfade und erwartete Typen, aber keine sensiblen
Istwerte. Nicht passende Typen werden im Binding gefiltert. Ein dennoch
zugestellter unpassender Typ ist ein sicher abzulegender Routingfehler; fehlt
einem Handler ein benötigtes Schema, ist dies ein Mappingfehler. Nicht explizit klassifizierte Handler-Exceptions werden
begrenzt wiederholt und anschließend abgelegt. Syntax-/Konfigurationsfehler
sind keine Nachrichten-Retries.

RPC ergänzt `RequestTimeoutException`, `RemoteCommandException`,
`InvalidReplyException` und `RpcNotConfiguredException`. Die lokal vom
Responder geworfene `CommandFailedException` beschreibt einen ausdrücklich
freigegebenen fachlichen Fehler; über den Rückkanal geht nur dessen sicheres
Fehlerobjekt. Ein Fehlerlevel in einer Begleitmeldung ist kein terminaler
Command-Fehler und ändert die Settlement-Entscheidung nicht.

### § 11.1 Wenn ein Callback eine Exception wirft

[Beispiel 10](../../examples/api-draft/10-callback-errors.php) zeigt die Fälle
direkt im Handler; [Beispiel 06](../../examples/api-draft/06-metadata-middleware.php)
zeigt das sichere Weiterwerfen nach Diagnose. Bei Auto-Ack bedeutet eine
Exception vor Settlement: kein Erfolgs-Ack. Die Runtime fängt behandelbare
`Throwable`s an der Handlergrenze ab und entscheidet nach folgender Policy.

| Callback-Ergebnis | Standard im Entwurf |
|---|---|
| Normale Rückkehr | Ack nach erfolgreicher Verarbeitung |
| `RetryableMessageException` | Begrenzter Retry mit Verzögerung; kein unendliches Erzwingen |
| Andere unbehandelte Exception, einschließlich `TypeError` | Ebenfalls begrenzt wiederholen; nach Ausschöpfen sichere Fehlerablage und Alarm |
| `RejectMessageException` | Sofort endgültig in die Fehlerablage, kein Retry |
| `CommandFailedException` oder freigegebene `RemoteException` (§ 13.5) in `respond` | Bewusster fachlicher RPC-Fehler: sichere finale Antwort, danach Ack; keine technische Wiederholung |
| Infrastrukturfehler bei Retry/Ack/FailureStore | Kein vorgetäuschter Erfolg; `run` wirft Infrastruktur-Exception, unbestätigte Nachricht bleibt wiederholbar |

Vorgeschlagene Default-Policy: höchstens vier Versuche insgesamt, also drei
Wiederholungen, mit 1, 2 und 4 Sekunden Verzögerung. Feste Wartequeues halten
den ersten Retry-Aufbau überschaubar; frei wählbarer Jitter ist nicht vorgesehen. `context->attempt` beginnt bei 1 und wird dauerhaft je Zustellung an
eine Subscription geführt; bestätigte Retry-Übergaben erhöhen den Zähler
dauerhaft. Prozessneustart setzt diesen Stand nicht zurück. Ein Crash vor
der Retry-Übergabe kann denselben Versuch wiederholen: vier Versuche sind eine
Grenze der regulären Handler-Retry-Runden, keine Exactly-once-Ausführungszählung. Die Policy bleibt über `SubscriptionOptions::retryPolicy`
austauschbar. Diese Defaults sind unsere Designentscheidung, keine Zusage des
Brokers. Ein laufender Retry blockiert nicht durch sleep den ganzen Worker;
der Job wird verzögert wieder verfügbar.

Nach endgültiger Ablehnung oder ausgeschöpften Versuchen wird zuerst der
FailureStore sicher bestätigt, dann die Ursprungszustellung beendet. Ohne
verfügbaren FailureStore kein Verwerfen und kein Ack; `FailureStoreException`
beendet den Loop. Der RabbitMQ-Adapter verwendet die zugehörige Fehlerqueue aus § 7. Fehlerdaten
enthalten ID, Subscription, Versuchszahl, Zeit und sichere Diagnose; Payloads
und Stacktraces sind nur in zugriffsgeschützter lokaler Ablage zulässig, nicht
ungefiltert in Events oder Frontend-Antworten. Manuelles Redrive erfolgt erst
nach Ursachenklärung mit erhaltenem Bezug und neuer expliziter Retry-Runde.

Bei technischen RPC-Fehlern wartet der Client über die zulässigen Retries.
Nach endgültigem Scheitern sendet die Runtime, soweit Rückkanal und Deadline
es noch erlauben, einen generischen sicheren `HANDLER_FAILED`-Fehler; der
Client erhält `RemoteCommandException`. Rohtexte unerwarteter Exceptions
werden nie übertragen. Fehlerablage und terminaler Reply-Publish werden vor
Request-Ack bestätigt; technische Fehler dabei bleiben wiederholbar. Bei
nicht erreichbarem Rückkanal kann stattdessen `RequestTimeoutException` beim
Client eintreten. Für eine bekannte fachliche `CommandFailedException` oder freigegebene
`RemoteException` ist keine technische FailureStore-Runde nötig; die sichere
Antwort bleibt aber zu bestätigen.

Ein behandelter Callback-Fehler beendet normalerweise nicht den Worker;
andere Jobs können weiterlaufen. Middleware darf die Exception loggen und
muss sie für korrekte Retry-/Ack-Entscheidung weiterwerfen. Prozesskill oder
Speichermangel sind nicht zuverlässig abfangbar: fehlendes Ack und das
Schließen des Delivery-Channels ermöglichen Recovery. Externe Seiteneffekte werden nicht zurückgerollt;
Idempotenz oder anwendungsseitige Transaktionen bleiben nötig. Ein bereits
manuell gesetztes Ack lässt sich durch eine spätere Exception nicht widerrufen.

Orientierung: [RabbitMQ – Acknowledgements](https://www.rabbitmq.com/docs/confirms)
unterscheidet Bestätigung, Requeue und Dead Letter. Unser Entwurf verbietet
stilles Verwerfen ohne sichere Ablage und begrenzt auch ausdrücklich retrybare
Fehler. Abruf 2026-09-12. Spätere Tests: Retry-Zählung über Neustarts, Fehlerablage
ausgefallen, Middleware schluckt/erhält Fehler, RPC-Endfehler und manuelles Ack.

## § 12 Paketgrenzen, spätere Prüfungen und Quellen

In die Library gehören Transportvertrag, Registry, Worker-Lebenszyklus,
Serialization, optionale Schema-Bridge, Security-/PayloadStore-Schnittstellen
und konsistente Exceptions. Der RabbitMQ-Adapter gehört zur ersten Implementierung; seine PHP-AMQP-
Abhängigkeit wird bei der Implementierungsplanung festgelegt. SDK-Verträge lassen sich unabhängig
von Brokerinstallationen verteilen.

Nicht in den Kern gehören fachliche DTOs, Business-Workflows, vollständige
Job-Scheduler, langfristige Workflow-/RPC-Ergebnisarchive, Cloud-Provisionierung,
Admin-UIs, Virenscanner, ZIP-Entpackung, PGP-Keyverwaltung oder eine eigene
verteilte Dateispeicherplattform. Erweiterungspunkte dürfen diese verbinden,
ohne den Grundvertrag damit zu belasten. Keine scheinbar universellen
Transaktionen, Prioritäten oder Exactly-once-Zusagen. Der nun beauftragte
RPC-Umfang bleibt eine optionale Request/Reply-Erweiterung gemäß § 13.

Globale Lock-/Konsensverfahren gehören nicht in die MQ-Library. § 15 zeigt
Broadcast und das Einsammeln von Lock-Bestätigungen; die tatsächlichen
lokalen Leases und gegebenenfalls ein autoritatives Fencing-Verfahren
verantwortet ein separater Lock-Dienst der Anwendung.

Für die spätere Umsetzung sind fokussierte Contract-Tests vorgesehen:
unabhängige Subscriptions versus Worker-Gruppe, Redelivery nach Crash,
Ack-Verlust, unbekanntes Publish-Ergebnis, lokale DTOs mit anderem Namespace,
verschachtelte Strukturen/required/null/zusätzliche Felder, manipulierte
Signaturen samt Metadaten, Rotation, Backlog-Zeitprüfung, fehlgeschlagene
Dateiprüfung, konkurrierende Consumer und Verbindungsabbrüche. Dieser
Entwurfs-PR fügt keine Laufzeitimplementierung oder Tests dafür hinzu.

Primärquellen, abgerufen am 2026-09-12:

- §§ 2–5, 7, 10: [RabbitMQ Queues](https://www.rabbitmq.com/docs/queues), [Exchanges](https://www.rabbitmq.com/docs/exchanges), [Confirms](https://www.rabbitmq.com/docs/confirms), [Quorum Queues](https://www.rabbitmq.com/docs/quorum-queues), [Management HTTP API](https://www.rabbitmq.com/docs/http-api-reference).

- § 6.1: [phore/schema Hydrator](https://github.com/phore/phore-schema/blob/aa8e60ab3b371fc3503f2a7ec8e2a3a63305074c/src/Hydrator/Hydrator.php), [Validator](https://github.com/phore/phore-schema/blob/aa8e60ab3b371fc3503f2a7ec8e2a3a63305074c/src/Validator/Validator.php), [Nutzungsinfo](https://github.com/phore/phore-schema/blob/aa8e60ab3b371fc3503f2a7ec8e2a3a63305074c/.ai-usage-info.md).

## § 13 RPC: Command, Rückgabewert und Begleitmeldungen

[Beispiel 05](../../examples/api-draft/05-rpc.php) enthält Verbindung,
programmatischen Responder, alternativ denselben Handler per `#[Respond]`,
Parameterübergabe, Ergebnis, Warning und Fehlerbehandlung in getrennten
Prozessen. `publish` und die optionale RPC-Komfortform `request`
veröffentlichen sofort und geben dasselbe `SendResult` zurück; erst dessen
`await()` blockiert auf die fachliche Antwort. Der Responder liefert mit `return` ein Array oder
DTO. Ein skalarer Wert wird explizit als `['value' => ...]` verpackt.

### § 13.1 Einmalige Konfiguration und Aufruf

`ConnectionOptions::rpc` nimmt `RpcConnectionOptions` entgegen. Bei
`enabled: true` erzeugt der Adapter vor dem ersten antwortfähigen Publish ein
zufällig eindeutiges Reply-Topic samt exklusiver, automatisch gelöschter Classic-Reply-Queue
pro Client unter `replyNamespace` (Demo `_phore.rpc`). Das ist eine ausdrücklich
aktivierte Ausnahme zur rein vorab angelegten fachlichen Topologie; passende
Configure-/Read-/Write-Rechte für den reservierten Bereich sind erforderlich,
auch bei `autoCreate: false`. Der Responder akzeptiert ausschließlich erlaubte
Reply-Ziele im konfigurierten Namespace; Zugang und Identität werden zusätzlich geprüft.

Der interne Reply-Consumer wird vor Publish eingerichtet, einschließlich des
Korrelationsregisters. Er teilt seine Queue niemals mit anderen Clients.
Der Adapter verwendet reguläre Reply-Queues, kein verlustbehaftetes Direct Reply-to.
Replies können nach Client-Verbindungsabbruch verloren gehen; offene Aufrufe
enden mit Verbindungsfehler oder Timeout. Dies ist kein dauerhaftes RPC-Ergebnisarchiv.
Konfigurierbare feste `replyTopic`/`replySubscription` bleiben für einen expliziten
zentralen Demultiplexer möglich; unabhängige Clients dürfen sie nicht gemeinsam
als konkurrierende Consumer verwenden. Der Server kann `allowedReplyTopics`
zusätzlich auf konkrete Ziele einschränken. Anzahl, Bytes und Lebensdauer des
Rückkanals bleiben begrenzt.

`RequestOptions` ergänzt `timeoutSeconds` (Default 30 Sekunden ab `request`,
nicht ab `await`), `metadata`, optional `responseClass` und `onNotice`.
`SendResult::await(?AwaitOptions $options = null, ?float $timeoutSeconds = null,
?string $responseClass = null, ?callable $onNotice = null,
?array $errorTypes = null): Reply` verarbeitet
nur den internen Rückkanal dieser
Connection, keine beliebigen Business-Handler. Mehrere Pending-Requests
teilen einen Dispatcher, der nach Request-ID puffert; Anzahl und Speicher
sind begrenzt. Gleichzeitige/nestende `run`-/`await`-Loops auf derselben
Connection sind ungültig. Für RPC aus einem Handler eine separate Connection
und einen unabhängig laufenden Responder verwenden.
`RequestOptions::responseClass`/`onNotice` liefern lediglich die Anfangswerte
für dieselben Await-Einstellungen. `PendingReply` entfällt als separater
Rückgabetyp im Entwurf; bestehende `request(...)->await()`-Beispiele bleiben
gültig.

`Reply` besitzt schreibgeschützte `payload`, `metadata` und `notices`.
`responseClass` hydriert `payload` strukturell nach § 6, ohne die PHP-Klasse
des Responders zu vergleichen. Ohne diese Option kommt ein Array zurück.
Metadaten und Begleitmeldungen werden nicht in das Ergebnis-DTO hineingemischt.
Ohne konfigurierte RPC-Schicht sind `request` und `respond` frühe
`RpcNotConfiguredException`s statt stiller Fire-and-forget-Aufrufe.

### § 13.2 Nachrichtenvertrag und Zustellverhalten

| Nachrichtenart | Body | Geschützte Metadaten |
|---|---|---|
| Request | Command-Parameter | `requestId` (= Request-`messageId`), `replyTo`, Deadline, `kind=request`, optionale fachliche `correlationId` |
| Result (`rpc.result.v1`) | Rückgabedaten | Ursprüngliche `requestId`, `kind=result`, eigene `messageId`, Antwortmetadaten und gesammelte Notices |
| Error (`rpc.error.v1`) | Sicheres Fehlerobjekt mit `code`, `message`, begrenzten `details` und optionalem `errorType` (§ 13.5) | `requestId`, `kind=error`, eigene `messageId`, gesammelte Notices |
| Notice (`rpc.notice.v1`) | `level`, `code`, `message`, begrenzte `details` | `requestId`, `noticeId`, eigene `messageId`, `kind=notice` |

Command-Typ und Subscription bestimmen eine logische Responder-Gruppe,
deren Worker die Arbeit teilen. Fan-out an mehrere unabhängig ausführende
Services ist für ein RPC-Command nicht der Standard und wird durch bewusst
provisionierte Topologie verhindert. Ein unbekanntes Command bleibt ein
Mappingfehler; es löst niemals Reflection-Aufrufe auf vom Sender benannten
PHP-Methoden, Shell-Kommandos oder Klassen aus.

Das erste passende verifizierte Result/Error beendet den Pending-Request.
Duplikate werden anhand Request-/Reply-ID behandelt; fremde oder verspätete
Antworten werden im eigenen Rückkanal nach konfigurierter Ablage-/Discard-
Policy bestätigt. Notices beenden den Request nicht. Der Client prüft
Signatur, Audience, Reply-Topic, Request-ID, erwarteten Antworttyp und Schema;
eine Korrelations-ID allein ist keine Authentifizierung.

`publish(...)->await()` und `request(...)->await()`
warten absichtlich nur auf eine terminale Antwort.
Wer Antworten aller Teilnehmer braucht, verwendet `publish` plus eine
aggregierende Subscription wie in § 15.2; hierfür wird keine mehrdeutige
`request(all: true)`-Option oder zusätzliche Queue-Methode eingeführt.

Auf dem Server folgen Antwort-Publish und dessen Bestätigung **vor** dem Ack
des Requests. Bei unklarer Antwortannahme bleibt der Request wiederholbar.
Ein Crash zwischen fachlicher Aktion, Reply-Publish und Request-Ack kann
mehrfache Ausführung/Replies erzeugen. Für verändernde Commands sind eine
idempotente Operation sowie ein persistenter Request-/Ergebnisspeicher mit
atomarer Anwendungsanbindung nötig: bekannte fertige Requests senden das
gespeicherte Ergebnis erneut, ohne die Aktion zu wiederholen. Dies ist keine
Exactly-once-Garantie der Queue und kein still aktivierter globaler Cache.

Ein Timeout begrenzt nur das lokale Warten: Der Server kann noch arbeiten
oder bereits fertig sein. Vor Handlerstart wird die geschützte Deadline
geprüft; während der Ausführung ist Abbruch kooperativ und keine Zusage.
Timeout führt nicht automatisch zu erneutem Senden. Offene Handles werden
bei `close` beendet; Reply-Retention und Bereinigung sind konfiguriert.
Abgelaufene Requests/Replies können in die lokale Fehlerablage gehen, ohne
noch einen rechtzeitig ankommenden Remote-Fehler versprechen zu können.

### § 13.3 Warnings, Fehler und Rückgabe-Metadaten

Der zweite Responder-Parameter ist `RequestContext`, eine Spezialisierung
von `MessageContext`. Er stellt genau zwei zusätzliche Operationen bereit:
`notify(Notice $notice): void` und `setReplyMetadata(array $metadata): void`.
`Notice(level: 'warning', code: ..., message: ..., details: ...)` sendet eine
Begleitmeldung, ohne Parameter oder Ergebnisstruktur zu ändern. `info` und
`error` sind ebenfalls zulässig; `error` als Notice kann etwa einen behobenen
Teilfehler melden und ist ausdrücklich nicht gleichbedeutend mit Abbruch.

`notify` sammelt eine begrenzte Notice-Liste und versucht die sofortige
Veröffentlichung am Rückkanal. Ein temporärer Notice-Publish-Fehler wird lokal
gemeldet und löst nicht allein eine erneute Command-Ausführung aus. Die
abschließende Result-/Error-Nachricht enthält die gesammelten Notices erneut,
damit verlorene oder überholte Zwischenmeldungen sichtbar bleiben. Beim
Erreichen des Limits folgt ein expliziter Truncation-Hinweis. Der Client
dedupliziert `onNotice` anhand `noticeId`; `Reply::notices` ist die finale
Zusammenfassung, keine zusätzlich ungefiltert auszugebende Ereignisliste.

`CommandFailedException` erzeugt eine terminale Fehlerantwort mit freigegebenem
Code und Text; `await` wirft daraus lokal `RemoteCommandException` mit
`errorCode`, `requestId`, sicheren Details und Notices. Fremde PHP-Exception-
Klassen/Stacks werden niemals übertragen oder instanziiert. Temporäre
Infrastrukturfehler folgen zunächst der begrenzten Retry-Policy; endgültige
unbekannte Fehler werden als neutraler `INTERNAL_ERROR` gemeldet und intern
abgelegt. Ein fehlerhafter `onNotice`-Callback darf den bereits laufenden
Command nicht erneut senden; er wird lokal gemeldet, der Dispatcher setzt
Empfang und Deadline-Verarbeitung fort.

### § 13.4 Sofort senden, anschließend optional warten

```php
$sent = $mq->publish($command); // Sendet sofort, wartet nur auf Broker-Annahme.
// Andere lokale Arbeit ...
$reply = $sent->await(timeoutSeconds: 5);

// Gleichwertig in einer Zeile, ohne zweites Sendekommando:
$reply = $mq->publish($command)->await(timeoutSeconds: 5);
$reply = $mq->publish('calculator', 'math.divide.v1', ['a' => 12, 'b' => 3])
    ->await(new AwaitOptions(timeoutSeconds: 5));
```

`Phore\MessageQueue\SendResult` hat eine readonly `receipt: PublishReceipt` und `await` mit der
Signatur aus § 13.1. Es ist ein bereits gesendeter Vorgang, kein verzögerter
Builder: weder `await` noch Destruktor oder `run` veröffentlichen ihn erneut.
`$mq->publish($event);` ohne weitere Verwendung sendet ebenso unmittelbar.
Fehlgeschlagene oder unklar bestätigte Broker-Annahme wirft bereits beim
Sendebefehl `PublishException`. Send-Middleware behält ihren internen
`PublishReceipt`-Vertrag; die Fassade ergänzt darüber das öffentliche
`SendResult`, ohne Middleware ein zweites Mal auszuführen.

Weil `await` erst nach dem Senden aufgerufen wird, müssen Antwortfähigkeit,
Reply-Ziel, Korrelation und Wire-Deadline bereits beim Publish feststehen.
`PublishOptions::reply` ist nullable: null (Default) aktiviert den Rückkanal
für fachliche Nachrichten automatisch, wenn die Connection einen vollständig
konfigurierten RPC-Client besitzt; false sendet ausdrücklich ohne Rückkanal;
true verlangt ihn und wirft bei fehlender Konfiguration **vor dem Senden**
`RpcNotConfiguredException`. Ohne RPC-Client sendet der Default normale
Events. Ein späteres `await` auf einem nicht antwortfähigen `SendResult`
wirft `ReplyNotEnabledException` und kann das gesendete Event nicht nachträglich
in einen Request verwandeln. Fehler im konfigurierten Rückkanal führen vor
Publish zum Fehler, nicht zu stillem Rückfall ohne Antwortfähigkeit.

Antwortfähige `publish`-Nachrichten tragen wie Requests signierte
`requestId`, `replyTo` und Deadline; ihr `kind=event` bleibt erhalten.
`respond` akzeptiert zusätzlich zu `kind=request` solche antwortfähigen
Events für seinen registrierten Typ und antwortet nach demselben Protokoll.
`subscribe` führt seinen Handler wie bisher aus und sendet auch bei gesetztem
Reply-Ziel **keinen** automatischen fachlichen Rückgabewert. Antwortfähigkeit
ändert weder Fan-out noch Worker-Gruppen. Interne Replies, Notices und
Health-Protokollnachrichten werden zwingend ohne neue Antwortanforderung
transportiert, damit keine Antwortschleifen entstehen.

`PublishOptions::replyTimeoutSeconds` bestimmt die vor dem Senden signierte
Antwortfrist (Default 30 Sekunden ab Sendebeginn). `expiresAt` kann die Frist
zusätzlich verkürzen. `Phore\MessageQueue\Rpc\AwaitOptions(timeoutSeconds, responseClass, onNotice, errorTypes)`
steuert dagegen ausschließlich das lokale Warten und die lokale
Ergebnisdarstellung. Direkte nicht-null Await-Parameter überschreiben das
Optionsobjekt wie bei `run`. Das effektive Warten endet am früheren Zeitpunkt
aus lokaler Wartefrist ab `await` und ursprünglicher Antwortdeadline; ohne
lokalen Timeout gilt die verbleibende Antwortfrist. Längere Remote-Fristen
müssen vor Publish gesetzt sein und lassen sich mit `await` nicht verlängern.
Ungültige Await-Optionen werfen `InvalidArgumentException`; die Nachricht
ist zu diesem Zeitpunkt ausdrücklich bereits gesendet.

Ein lokaler Timeout oder Ablauf der ursprünglichen Antwortdeadline ohne
rechtzeitiges finales Ergebnis wirft `RequestTimeoutException` mit `requestId`,
niemals null/false oder ein leeres Erfolgs-Reply; der entfernte Handler wird
nicht abgebrochen. Notices setzen keine Frist zurück. Solange die ursprüngliche Antwortfrist läuft, darf derselbe
Handle erneut warten, ohne erneutes Senden; eine bereits verifizierte terminale
Antwort wird bis zur Handle-Freigabe zwischengespeichert. Ein nach Ablauf erst
eintreffendes Result wird nicht mehr als rechtzeitige Antwort akzeptiert.
Bereits rechtzeitig empfangene finale Ergebnisse bleiben abrufbar. Die erste
Await-Ausführung fixiert `responseClass`, `errorTypes` und Notice-Callback für diesen Handle;
widersprüchliche spätere Änderungen sind ungültig, ein neuer lokaler Timeout
ist erlaubt. Notices werden pro Handle dedupliziert; vor `await` empfangene
Notices bleiben nur im begrenzten Puffer, dessen Overflow explizit gemeldet
wird, und sind nach Möglichkeit zusätzlich im finalen Reply enthalten.

Der Client puffert nur begrenzt viele offene Vorgänge/Antwortbytes. Eine
erschöpfte Kapazität wird vor einem weiteren antwortfähigen Publish als
`PendingCapacityExceededException` gemeldet. Freigegebene Handles geben ihre
lokalen Slots frei; später eintreffende Replies werden sicher verworfen bzw.
nach der konfigurierten Ablagepolicy behandelt. Deadline und `close` räumen
verbliebene offene Vorgänge auf. Auch ohne `await` müssen Rückkanal-Retention
und Ressourcenlimits wirken; keine unbegrenzte Hintergrundwarteschlange und
kein impliziter Hintergrundthread. Bewusst reine Events können mit
`PublishOptions(reply: false)` den Antwortaufwand vermeiden.

Bei fehlendem Responder oder einem reinen `subscribe`-Empfänger endet `await`
mit Timeout; Schweigen beweist nicht, dass niemand existiert. Ein gezielter
Health-Check kann vorab Bereitschaft prüfen, aber die Antwort nicht garantieren.
Ein Broker-Ack ist kein RPC-Ergebnis. Wer Antworten aller Subscriber benötigt,
verwendet weiter die explizite Aggregation aus § 15.2. `request` bleibt die
Komfortform, die Antwortfähigkeit zwingend verlangt, `kind=request` setzt und
`RequestOptions::timeoutSeconds` als ursprüngliche Remote-Frist übernimmt;
eine zweite Promise-/Worker-API entsteht dadurch nicht.

Vorgesehene Contract-Tests: genau ein Publish mit/ohne/nach mehrfachem await,
Antwort vor await, lokaler Timeout und späteres Result, unverlängerbare
Wire-Deadline, fehlender Rückkanal/Responder, Notice-Puffergrenze, ignorierte
Handles, Kapazitätsgrenze, interne Antworten ohne Rekursion und direkte
Parameter versus Optionsobjekt. Beispiel 05 zeigt beide Sendeformen.

### § 13.5 Eigene RPC-Exceptions mit freigegebener Meldung

Der einfache Weg bleibt `throw new CommandFailedException(errorCode: ...,
publicMessage: ...)` im Responder und `catch (RemoteCommandException $e)` um
`await`. Es gibt keine Anwendungspflicht, Fehlernachrichten zu abonnieren oder
einen Fehler-Payload manuell auszuwerten; die Runtime verarbeitet das interne
`rpc.error.v1` und wirft lokal eine Exception. Timeout ist weiterhin eine
separate `RequestTimeoutException`.

Für typisierte SDK-Fehler ist `#[RemoteError('math.division_by_zero.v1')]`
auf einer konkreten Unterklasse von `RemoteException` vorgesehen.
`RemoteException` erbt von `RemoteCommandException` und markiert explizit
für Übertragung freigegebene Fehler. Sein gemeinsamer Konstruktor lautet
`__construct(string $message, string $errorCode = 'REMOTE_ERROR', array $details = [])`;
SDK-Unterklassen überschreiben ihn nicht und benötigen keine zusätzlichen
Pflichtfelder. Der Server wirft beispielsweise
`new DivisionByZero('Division durch null ist nicht möglich.')`. Die Meldung
ist bewusst öffentlich; sensible Rohmeldungen dürfen nicht hineinkopiert werden.

Der Client erlaubt lokale Klassen mit
`await(errorTypes: [DivisionByZero::class])` oder
`await(new AwaitOptions(errorTypes: [...]))`. Das ist eine lokale Allowlist,
keine Liste vom Sender. Der Resolver ordnet den stabilen RemoteError-Namen der
Klasse zu; doppelte Namen, ungeeignete Klassen/Konstruktoren oder fehlende
Attribute sind ungültige Await-Konfiguration. Ein gemeinsames SDK liefert
dieselbe Exception-Klasse auf beiden Seiten; alternativ darf der Client eine
anders benannte lokale Unterklasse mit demselben Fehlernamen erlauben.
Ohne passenden Eintrag wird `RemoteCommandException` mit derselben sicheren
Meldung geworfen. Typisierte Fehler sind deshalb auch generisch fangbar.

Auf dem Wire bleibt es ein begrenztes Fehlerobjekt mit `errorType`, `code`,
`message`, freigegebenen JSON-`details` und verifizierter Request-Zuordnung.
Die Factory erzeugt ausschließlich einen lokal erlaubten Exception-Typ und
setzt den Request-Kontext aus der verifizierten Antwort. PHP-FQCN, Trace,
`previous` und beliebige Objektproperties werden nicht übertragen; kein
`unserialize` und keine allgemeine Throwable-Hydration über phore/schema.
`RemoteError` ist ein besonderer Fehlervertrag, kein `MessageType`, den
`publish` als normalen Event automatisch versendet. Erst das Werfen im
Responder löst die terminale Fehlerantwort aus.

Nur eine ausdrücklich deklarierte `RemoteException` oder die bestehende
`CommandFailedException` darf den vorgesehenen sicheren Text exportieren.
Beliebige `Exception`-/`RuntimeException`-Unterklassen oder vom Handler
weitergeworfene generische Remote-Fehler werden nicht automatisch freigegeben:
für sie gelten begrenzter Retry und generisches `HANDLER_FAILED` aus § 11.1.
Typed Errors sind terminale fachliche Antworten ohne Retry; Veröffentlichung
vor Ack bleibt erforderlich. Ein kaputtes/unerlaubtes Fehlerframe wird nicht
als erfolgreiche Antwort oder als frei gewählte lokale Exception behandelt.

Beispiel 05 enthält Server-Throw, generischen Client-Catch und typisierten
Client-Catch einschließlich Meldung. Vorgesehene Tests: gleiche/andere lokale
Klasse, unbekannter Fehlername, doppelte Allowlist-Namen, keine Offenlegung
technischer Rohfehler, manipulierter Fehlerframe, Timeout versus Remote-Fehler
und wiederholtes await ohne erneute Ausführung des Commands.

## § 14 Metadaten, Middleware und API-Entscheidung

[Beispiel 06](../../examples/api-draft/06-metadata-middleware.php) zeigt
Trace-/Locale-Metadaten, eine Send-Middleware, eine Handler-Middleware und
das eigenständige Publizieren von Warnungen/Fehlern auf ein Diagnose-Topic.
Das ist sowohl mit normalen Events als auch mit RPC nutzbar.

### § 14.1 API-Entscheidung

Die öffentliche API bleibt klein und erklärt die Wirkung am Aufruf:
`publish` sendet sofort, `await` wartet auf eine Antwort, `subscribe` und
`respond` registrieren Handler, `run` verarbeitet Zustellungen. Ein zentrales
`PhoreMQ` bündelt Konfiguration und Lebenszyklus. Fachliche DTOs tragen optionale
Metadaten; Arrays und explizite Topic-/Typ-Angaben bleiben gleichwertig möglich.
RabbitMQ-spezifische Klassen werden nur bei direkter Adapter-Injektion benötigt.

### § 14.2 Metadaten außerhalb des fachlichen Payloads

`PublishOptions(metadata: [...])` und `RequestOptions(metadata: [...])`
tragen eine begrenzte JSON-kompatible Map. `MessageContext::metadata` ist
die schreibgeschützte Empfangssicht. Schlüssel unter `app.*` sind für die
Anwendung vorgesehen, etwa `app.traceId`, `app.locale`, `app.tenantId` oder
`app.source`. Systemfelder wie `requestId`, `replyTo`, Typ, Audience,
Deadline und Signatur dürfen darüber nicht überschrieben werden. Grenzen
für Schlüsselzahl, Tiefe und Bytes werden vor Versand geprüft.

Metadaten werden mit signiert; ihre Integrität ist damit geschützt, sie sind aber nicht automatisch
autorisiert. Tenant-/Benutzerangaben müssen gegen die authentifizierte
Verbindung/Identität geprüft werden. Kein implizites Weiterreichen von
Tokens oder sämtlichen eingehenden Metadaten. Antwortmetadaten werden
ausdrücklich gesetzt, etwa `app.worker` oder `app.durationMs`. Allgemeine
Warnings/Errors sind normale `diagnostic.v1`-Events mit Level, Code und
sicherem Text; ihre `correlationId` verbindet sie mit dem betreffenden Request.
Es gibt dafür keine zusätzliche Spezialmethode auf der Queue-Fassade.

### § 14.3 Genau zwei optionale Middleware-Hooks

`ConnectionOptions(sendMiddleware: [...], handleMiddleware: [...])` akzeptiert
Callables oder invokable Objects. Es braucht weder Basisklasse noch Service-
Locator. Die Verträge lauten `send(OutgoingMessage $message, callable $next):
PublishReceipt` und `handle(mixed $payload, MessageContext $context,
callable $next): mixed`. Das sind Callable-Signaturen, keine zusätzlich
aufzurufenden Queue-Methoden. `$next` wird im Normalfall genau einmal aufgerufen;
Exceptions propagieren. Rückgabewerte dürfen nicht verloren gehen.

Send-Hooks laufen vor finaler Schema-/Envelope-Prüfung und Signierung;
`withMetadata` erzeugt eine neue lokale Nachricht mit zusammengeführten
Anwendungsmetadaten. Gesendete Retries verwenden die bereits geschützten
Bytes, ohne neue Trace-IDs oder Zeitstempel einzumischen. Handler-Hooks laufen
nach Signaturprüfung und Hydration, aber vor Result-/Error-Erzeugung und Ack;
sie ändern keine geschützten Eingangsbytes. Die Reihenfolge folgt der
Registrierung, mit Rückweg in umgekehrter Reihenfolge. Die mandatory
Security-/Settlement-Schritte sind keine entfernbaren Nutzer-Middleware.

Die Diagnose-Middleware publiziert über eine separate, bereits konfigurierte
Connection ohne dieselbe Diagnose-Middleware, damit kein Fehler-Event wieder
ein Fehler-Event auslöst. Sie verwendet verifizierte Kontextdaten, redigiert
Texte und wirft den ursprünglichen Fehler erneut; der Fehler wird nicht
versehentlich als Erfolg bestätigt. Ein fehlgeschlagenes Diagnose-Publish
meldet sie lokal. Für garantiert vollständige Diagnosezustellung braucht es
eine persistente Outbox; ein erfolgreicher Business-Callback wird nicht nur
wegen eines Telemetriefehlers wiederholt.

### § 14.4 Sicherheitsgrenzen und spätere Prüfung

`replyTo` bezeichnet nur ein lokales logisches Topic aus der autorisierten
Allowlist, niemals eine DSN, URL oder vom Request gelieferte neue Verbindung.
Der Server prüft Reply-Zielberechtigung und Nachrichtenlimits vor Handlerstart;
im Mehrmandantenbetrieb gilt die Allowlist pro authentifiziertem Principal.
Der gemeinsame HMAC-Key des Beispiels allein trennt keine Mandanten. Replies,
Notices und Diagnose-Events benutzen dieselbe Schutzkette wie Events.
Fehlende Signaturen erhalten keine Antwort an untrusted Rückkanäle.

Zusätzliche spätere Contract-Tests: unmittelbare Antwort vor Beginn von
`await`, parallele Request-IDs, doppelte/späte Replies, Timeout bei laufendem
Command, unzulässiges Reply-Topic, Warnung vor/nach Ergebnis, Notice-Duplikate,
Notice-/Diagnose-Publish-Fehler, Fehler im Notice-Callback, Schemafehler im
Result, Weitergabe eines Middleware-Rückgabewerts und Rückwurf der
ursprünglichen Exception. Keine solchen Laufzeittests in diesem Entwurfs-PR.

Quellen für §§ 13–14, abgerufen am 2026-09-12:

- [RabbitMQ: RPC mit PHP](https://www.rabbitmq.com/tutorials/tutorial-six-php).

## § 15 An alle Subscriber oder an einen Worker

### § 15.1 Dasselbe Topic, bewusst gewählte Subscriptions

| Ziel | Subscription-Namen | Ergebnis |
|---|---|---|
| Alle beteiligten Dienste informieren | `locks-service-a`, `locks-service-b`, `locks-service-c` | Jeder Dienst erhält eine Kopie und kann separat antworten |
| Alle konkreten Instanzen informieren | Je Instanz ein stabiler eigener Name, etwa `locks-instance-17` | Jede erwartete Instanz erhält ihre eigene Kopie |
| Einen Job verteilen | Alle Worker: `text-processors` | Ein verfügbarer Consumer erhält die konkrete Zustellung zur Bearbeitung |

Ein Topic kann beides gleichzeitig haben, etwa eine Worker-Subscription und
eine unabhängige Audit-Subscription. „An einen“ bedeutet deshalb nicht
weltweit exklusiv, falls daneben weitere Subscriptions existieren. Für die
Processing-Queue provisioniert man bewusst nur die ausführende Worker-Gruppe;
Audit-Consumer führen den Job nicht aus. Die ersten beiden Muster brauchen
die RabbitMQ-Bindungen: Jede unabhängige Subscription besitzt ihre eigene Queue.

### § 15.2 Lock-Koordination: alle bekannten Teilnehmer antworten

[Beispiel 07](../../examples/api-draft/07-broadcast-locking.php) verwendet
`publish('maintenance.locks', 'lock.acquire.v1', ...)`. Jeder Teilnehmer
besitzt eine eigene Subscription auf diesem Topic, nimmt eine lokale Lease
für seine Ressource und sendet `lock.state.v1` an den Rückkanal. Der Koordinator
abonniert den Rückkanal vor dem Broadcast und zählt **Teilnehmer-IDs**, keine
Nachrichtenanzahl. Doppelte Antworten erhöhen den Zähler nicht. Erst alle
positiven Antworten der festen Teilnehmerliste erlauben den nächsten Schritt.

Die Liste ist ein Membership-Snapshot aus der Anwendungskonfiguration, keine
aus Queue-Subscriber-Zahlen erratene Größe. Erwartete Subscriptions werden
vor dem Lauf angelegt. Offline-Teilnehmer bleiben erwartet und führen zum
Timeout; neue Teilnehmer gehören erst zur nächsten Runde. Eine negative
Antwort oder die Akquise-Deadline bricht die Runde ab und löst einen
`lock.release.v1`-Broadcast aus. Jede Runde besitzt eine eindeutige ID;
alte/fremde Antworten werden verworfen. Je Rückkanal läuft nur ein
zuständiger Koordinator oder ein expliziter Demultiplexer.

Der gezeigte `LocalLeaseManager` ist eine **Anwendungsabhängigkeit**, keine
MQ-API. Erwerb ist idempotent pro Ressource/Runden-ID, hat eine absolute
begrenzte Gültigkeit und verlängert sich bei Redelivery nicht. Release gibt
nur die eigene Runde frei und hinterlässt bis zum Ablauf eine Abschlussmarke,
damit verspätete Acquire-Nachrichten einen freigegebenen Lock nicht erneut
nehmen. Leases laufen unabhängig vom Queue-Worker ab; dadurch bleiben bei
Koordinator-Crash oder verlorenem Release keine unbegrenzten Locks zurück.

„Alle haben ihren lokalen Lock bestätigt“ ist eine koordinierte Barriere,
kein Beweis eines linearisierbaren globalen Locks oder dauerhafter Gesundheit
aller Teilnehmer. Die kritische Arbeit muss vor der kleinsten sicheren
Lease-Deadline enden; Clock-Skew und Ausführungszeit brauchen Reserve. Ein
einfacher Zeitvergleich in PHP verhindert keine Pause nach dem Vergleich.
Für geschützte Schreibzugriffe muss die Zielressource deshalb veraltete
Operationen über einen autoritativen monotonen Fencing-Token ablehnen.
Die Runden-ID ist nur Korrelation/Ownership, kein solcher Fencing-Token.
Quorum/Konsens, Membership-Änderung und Lease-Verlängerung sind hier bewusst
keine Behauptung der Queue-Abstraktion.

Teilnehmer-IDs aus Reply-Payloads sind allein nicht vertrauenswürdig. Das
Beispiel setzt kooperative Teilnehmer mit gemeinsamer Entwicklungs-Identität
voraus. Produktion muss jede Antwort einer erlaubten Teilnehmeridentität
zuordnen, etwa über getrennte Signing-Keys/Principals oder getrennte
Reply-Topics mit durchgesetzten Publisher-ACLs. Ein gemeinsamer HMAC-Key
beweist nicht, welcher Teilnehmer tatsächlich den Lock besitzt.

### § 15.3 Processing-Queue: ein Worker verarbeitet und antwortet

[Beispiel 08](../../examples/api-draft/08-processing-workers.php) startet
mehrere Prozesse mit `respond('jobs.text', 'text-processors', ...)`.
**Der Subscription-Name bleibt bei allen Workern identisch.** Der Adapter
erzeugt getrennte Transport-Consumer-IDs; eine Worker-ID dient im Beispiel
nur als Antwortmetadatum, nicht als neue Subscription. Der Client ruft
`request('jobs.text', 'text.process.v1', $params)->await()` auf und erhält
Payload und die Kennung des verarbeitenden Workers zurück.

RabbitMQ verteilt an verfügbare Consumer unter Berücksichtigung ihres Prefetch-Limits. „Random“ wird hier als „beliebiger verfügbarer Worker,
ohne feste Zielinstanz“ verstanden. Gleichmäßiger Zufall, Round-robin oder
garantierte Fairness sind kein portabler Vertrag; auch mehrere Jobs
hintereinander beim selben Worker sind zulässig. Wer eine bestimmte
Verteilungsstrategie benötigt, braucht einen gesonderten Scheduler.

Pro Zustellversuch wird ein Consumer ausgewählt; ein normaler Job wird
nicht an alle Worker kopiert. Bei Crash, verlorenem Ack oder Verbindungsabbruch
kann derselbe Job dennoch erneut zugestellt werden. Ein pausierter alter
Worker kann nach Verlust seines Channels sogar noch weiterlaufen, während ein neuer
übernimmt. „Nur ein Worker“ ist daher keine Exactly-once-/Seiteneffektgarantie:
lange Verarbeitung braucht passende Ack-Fristen und Heartbeat-Verarbeitung, kritische Aktionen benötigen
Idempotenz oder ressourcenseitiges Fencing. Das Beispiel verarbeitet reinen
Text ohne externe Seiteneffekte; Ergebnis-Publish erfolgt gemäß § 13 vor
Request-Ack.

### § 15.4 Spätere Prüfungen und Quellen

Vorgesehene Contract-Tests: Broadcast an drei Subscriptions versus drei
Worker einer Gruppe, doppelte Teilnehmerantworten, fehlender/negativer
Teilnehmer, spätes Acquire nach Release, Koordinator-Crash, veraltete Lease,
falsche Teilnehmeridentität und erneute Job-Ausführung nach Verbindungsabbruch.
Diese Tests gehören zur späteren Implementierung, nicht zum Entwurfs-PR.

- [RabbitMQ Consumers: konkurrierende Consumer und Zustellsteuerung](https://www.rabbitmq.com/docs/consumers).

Abruf: 2026-09-12; die konkrete API und die Barrierenlogik sind der
hier vorgeschlagene Anwendungsentwurf.

## § 16 Standardisierter Systemcheck und Dienststatus

### § 16.1 Ziel und kleine Schnittstelle

Frontend-Backends und Monitoring sollen frühzeitig erkennen, ob die für eine
Nachricht benötigten Dienste bereit sind, und dieselbe verständliche Ursache
erhalten. Ein bekanntes Berechtigungsproblem soll den betroffenen Handler
deaktivieren können, während der Prozess seinen Zustand weiterhin meldet.
Der Check ist eine zeitlich begrenzte Bereitschaftsaussage; er kann spätere
Laufzeitfehler oder einen Ausfall unmittelbar nach der Prüfung nicht ausschließen.

```php
$connection = $mq->check(); // Verbindung + lokale Konfiguration, keine Consumer-Zusage.
$system = $mq->check('jobs.text', 'text.process.v1'); // Erwartete Consumer aus Konfiguration.
if (!$system->ready) {
    // Funktion im Frontend als momentan nicht verfügbar kennzeichnen.
}
```

Die vorgeschlagene Signatur ist `check(?string $topic = null, ?string $type =
null, ?CheckOptions $options = null): HealthReport`. Topic und Typ werden
gemeinsam angegeben oder gemeinsam weggelassen. Mit
`check(options: new CheckOptions(requireDeclared: true))` werden sämtliche
in `HealthOptions::requirements` deklarierten Nachrichtenabhängigkeiten
unter einem gemeinsamen Zeitbudget geprüft; ohne diesen ausdrücklichen
Schalter bleibt `check()` der reine Verbindungscheck. Eine leere
Anforderungsliste bei `requireDeclared: true` ist ein Konfigurationsfehler.
`requireDeclared: true` zusammen mit einem expliziten Topic/Typ ist ungültig. `CheckOptions` enthält
`timeoutSeconds` (Default 3 Sekunden Gesamtbudget), optional
`requiredSubscriptions`, `minReadyPerSubscription` (Default 1) und
`requiredInstances`. Eine `ReadinessRequirement` in `ConnectionOptions::health`
legt diese Anforderungen je Topic/Typ einmalig fest. Fehlt eine erwartete
Topologie, meldet der gezielte Check `EXPECTED_CONSUMERS_UNDEFINED`/unknown;
eine leere Antwortliste ist niemals automatisch ein gesunder Zustand.

Der reine Verbindungscheck verwendet nur native, nicht verändernde
Operationen. Ein gezielter Check erzeugt begrenzte Probe-/Reply-Nachrichten
im reservierten Health-Kanal, aber keine fachlichen Jobs, Test-Logins,
Dateiuploads oder Handler-Aufrufe. `autoCreate: false` bleibt wirksam: fehlende
Health-Ressourcen werden gemeldet und nicht im Check heimlich angelegt.
Ein fehlgeschlagener initialer `connect`-Aufruf bleibt eine Connection-/Auth-
Exception; `check` untersucht eine bereits erzeugte Connection erneut.

### § 16.2 Was geprüft wird und welche Aussage daraus folgt

| Prüfung | Positive Aussage | Grenzen / mögliche Probleme |
|---|---|---|
| Verbindung | Native Brokeranfrage mit den konfigurierten Zugangsdaten gelingt | Kein Nachweis für Publish-/Consume-Rechte auf allen Topics |
| Lokale Konfiguration | Mapping, installierter Connector, Schema-Metadaten und Security-Konfiguration sind auflösbar | Keine Ausführung eines DTO-Konstruktors/Business-Handlers als Probe |
| Ziel-Topologie | Topic/Subscription/Binding existieren, soweit der Adapter sie prüfen darf | Ohne Prüfmöglichkeit/Rechte unknown, niemals erfundener Erfolg |
| Consumer-Bereitschaft | Aktuelle Antworten der erwarteten Gruppen/Instanzen, registrierter Handler für Typ, aktive Consume-Bindung und keine blockierende Störung | Consumer-Zähler allein reichen nicht |
| Anwendungsabhängigkeiten | Benannte Prüfungen melden z. B. Datenbank, Ausgabeverzeichnis oder Fremddienst bereit | Nur tatsächlich geprüfte Abhängigkeiten; Probe muss seiteneffektfrei sein |
| Betriebsprobleme | Optionale, aktuelle Werte für Rückstau, älteste Nachricht, Pending/Retry/Dead Letter und letzte Fehler | Schwellen konfiguriert; nicht messbare Werte sind null/unknown |

Ein erfolgreicher Health-Roundtrip beweist den Health-Pfad. Er beweist nicht
automatisch den fachlichen Publish-Pfad, dessen Berechtigungen oder die
Kompatibilität jeder späteren Payload. Der Bericht nennt deshalb jede
Prüfung mit Quelle, Zeitpunkt und Grenzen. Schema-/Versionsinformationen
werden als strukturelle Fähigkeit gemeldet; unterschiedliche lokale PHP-
Klassennamen oder Schema-Fingerprints sind allein kein Inkompatibilitätsfehler.

Für eine Processing-Gruppe reicht standardmäßig ein frischer bereiter Worker
pro erwarteter Subscription. Für Broadcast müssen alle erwarteten
Subscriptions bereit sein. Sind konkrete Instanzen zwingend, werden ihre
IDs zusätzlich als fester Snapshot vorgegeben. Ein gesunder Ersatzworker
genügt dann nicht anstelle einer ausdrücklich verlangten Instanz. Ein
ausgefallener optionaler Worker bei erfüllter Mindestkapazität kann einen
degraded-Bericht mit `ready=true` erzeugen.

### § 16.3 Gemeinsamer Dienstzustand: aktiv melden und auf Ping antworten

`HealthState(serviceId, instanceId)` besitzt die Operation
`set(string $check, HealthFinding $finding, ?string $topic = null, ?string
$type = null): void`. Topic und Typ müssen auch hier gemeinsam gesetzt oder weggelassen werden.
Ein benannter Check wird ersetzt statt als unendliche
Fehlerliste angehängt. `HealthFinding::healthy`, `degraded`, `unhealthy` und
`unknown` erzeugen typisierte Befunde mit stabilem Code, sicherem
`publicMessage` und einer konkreten Handlungsempfehlung `action`. Ohne
Topic/Typ betrifft der Befund den ganzen Dienst, sonst nur passende Handler.
Abhängigkeiten beginnen unknown, bis eine echte Prüfung vorliegt.

`HealthOptions(state: $health, refresh: $probe, refreshIntervalSeconds: 5,
statusTtlSeconds: 20)` bindet das lokale Zustandsobjekt ein. `refresh` ist
ein begrenzter, seiteneffektfreier Callback `callable(HealthState): void`, der
die Befunde aktualisiert. Callback-Exceptions werden als `HEALTH_PROBE_FAILED`
erfasst; sichere Details bleiben lokal. Ein gleichbleibender Fehler wird
dedupliziert, bei Änderung entsteht unmittelbar eine Statusmeldung, zusätzlich
gibt es begrenzte Aktualisierungen mit Ablaufzeit. Die Zahlen sind
Entwurfsdefaults, keine universellen Betriebsintervalle. Vor dem Binden an
eine Connection aktualisiert `set` nur den lokalen Zustand; beim Start wird
dieser veröffentlicht. Synchrones PHP kann einen hängenden Probe-Callback
nicht präemptiv abbrechen: Abhängigkeitsclients müssen eigene kurze Timeouts
einhalten. Harte Ausführungsgrenzen benötigen Prozessisolation.

Registrierte `subscribe`-/`respond`-Handler, tatsächliche Consume-Bindungen
und deren Zustand werden von der Library ergänzt. Eine Anwendung darf mit
`set(...healthy...)` eine fehlende Registrierung, Authentifizierung oder
geschlossene Verbindung nicht überstimmen. Probe-Antworten und aktive
`system.health.v1`-Events stammen aus derselben Snapshot-Erzeugung; ein
Frontend muss keine unterschiedlichen Fehlerformate je Dienst verstehen.

Ein negativer Pflichtbefund pausiert nur die betroffene Verarbeitung. Der
Worker bleibt im Health-/Recovery-Loop erreichbar, prüft mit Backoff erneut
und nimmt Arbeit erst nach erfolgreicher Wiederherstellung an. Bei einem
Berechtigungsfehler wird also nicht einfach der Container beendet. Tritt
der Fehler nach der letzten Prüfung im Handler auf, setzt die Anwendung
den gleichen Befund und wirft eine passende Retry-/Reject-Exception; die
Library bestätigt die fehlgeschlagene Verarbeitung nicht als Erfolg.

Die Runtime fragt für pausierte Ziele keine neuen Jobs ab. Bereits zugestellte
Nachrichten werden nach der Retry-Policy verzögert freigegeben oder
begrenzt gehalten, nicht engmaschig konsumiert und erneut veröffentlicht.
Health-Probes sind davon getrennt. Unterbrechungsschutz, sichere Fehlerablage
und die bestehenden Retry-Grenzen bleiben wirksam.

### § 16.4 Health-Kanal, Ausfälle und Authentifizierung

Health ist eine reservierte Erweiterung mit `system.health.probe.v1`,
`system.health.reply.v1` und `system.health.v1`. Ein Probe enthält Probe-ID,
Ziel-Topic/-Typ, erwartete Subscriptions/Instanzen und Deadline. Pro Instanz
gibt es eine eigene Control-Subscription; Antworten werden je verifizierter
Instanz und Probe-ID aggregiert, nicht wie normales RPC nach der ersten
Antwort beendet. Pflichtinstanzen werden nicht aus Antwortzahlen erraten.
Interne Health-Nachrichten durchlaufen Signierung und Größenlimits, aber
keine fachlichen Handler oder rekursiv meldende Diagnose-Middleware.

Die Standardeinstellungen benutzen einen reservierten konfigurierbaren
Namespace, etwa `_phore.health.probes`, `_phore.health.status` und
`_phore.health.replies.<monitorId>`. Reply-Ziele sind lokal provisionierte,
berechtigte Namen, niemals DSNs aus einem Probe. Zugriffe, Service- und
Instanzzuordnung müssen über verifizierte Identitäten/Keys beziehungsweise
Broker-ACLs begrenzt sein. Ein gemeinsamer Entwicklungs-HMAC-Key ist keine
verlässliche Identität einzelner Dienste oder Mandanten.

Verliert ein Dienst die Berechtigung für seinen fachlichen Kanal, kann er
über einen separat berechtigten Health-Kanal weiter antworten. Verliert er
auch diesen Zugang, kann er den Fehler **nicht über genau diese defekte
Verbindung zuverlässig melden**. Der Monitor markiert nach TTL/Deadline
den Zustand unknown/stale, mit letzter bekannter Ursache ausdrücklich als
historischer Information. Eine aktuelle Ursache ist dann nicht automatisch
bestimmbar; Timeout darf nicht als bewiesener Rechtefehler ausgegeben werden.

Für unabhängig verfügbare Diagnose kann `HealthOptions::controlConnection`
eine explizit injizierte separate Verbindung mit begrenzten Rechten verwenden.
Ein `HealthOptions`-gebundener reiner Diagnose-Client kann über `run()` den
Health-State auch dann bedienen, wenn der Aufbau der Business-Connection
scheitert; die Anwendung trägt deren bekannte Fehlerursache in den geteilten
State ein. Für Totalverlust der MQ-Infrastruktur kann derselbe Snapshot
über einen externen HTTP-/Monitoring-Adapter exportiert werden. Weder
HTTP-Server noch neue Zugangsdaten werden implizit erzeugt.

Eine zusätzliche Socket-Verbindung bedeutet keine parallele PHP-Ausführung.
Ein synchron blockierter Handler kann Probe-Antworten verzögern. Die
Health-Runtime kann zwischen Jobs und in Recovery-Phasen antworten; für
Antworten während beliebig langer blockierender Arbeit ist ein separat
überwachter Prozess oder ein ausdrücklich unterstütztes asynchrones
Laufzeitmodell erforderlich. Auch der Checker konsumiert ausschließlich
seinen internen Rückkanal; reentrante `check`-/`run`-/`await`-Loops auf derselben
Connection sind ungültig.

### § 16.5 Versioniertes Ergebnis und stabile Fehlercodes

`HealthReport` ist eine schreibgeschützte DTO-Struktur mit `toArray()` für
JSON-Ausgabe. Probe- und Push-Berichte teilen denselben Vertrag:
`reportVersion`, `reportId`, `kind` (instance/aggregate), `scope`, `checkedAt`,
`expiresAt`, `durationMs`, `status`, `ready`, `checks`, `consumers`, `issues`.
Ein Einzelbericht hat zusätzlich `serviceId`, `instanceId`, `generation`
(zufällige Start-ID) und `sequence` (monoton innerhalb dieser Generation).
Ein Aggregate enthält die ausgewerteten Einzelberichte/Referenzen.
Ein Check aller deklarierten Abhängigkeiten hat `scope=system` und zusätzlich
`targets`: eine Liste vollständiger Zielberichte mit `topic` und `type`.
Ein gezielter Bericht hat `scope=message` mit diesen beiden Zielfeldern.
`ready` des Systemberichts setzt alle als erforderlich deklarierten Ziele
voraus; eine frontendseitig optionale Funktion bleibt einzeln auswertbar.

| Status | Bedeutung | Einfluss auf `ready` |
|---|---|---|
| `healthy` | Alle angeforderten Pflichtprüfungen aktuell positiv, keine bekannten Zusatzprobleme | true für den ausgewiesenen Scope |
| `degraded` | Pflichtanforderungen erfüllt, aber Warnung, optionale Messlücke oder reduzierte Redundanz | true, solange keine blockierende Schwelle überschritten ist |
| `unhealthy` | Mindestens eine Pflichtanforderung nachweislich verletzt | false |
| `unknown` | Mindestens eine Pflichtanforderung ungeprüft, nicht beantwortet oder veraltet | false |

Aggregationsreihenfolge: nachgewiesener Pflichtfehler vor Pflicht-Ungewissheit,
danach degraded und healthy. Findings optionaler Prüfungen blockieren keine
erfüllte Bereitschaft, bleiben aber als Problem sichtbar. Ein Brokerfehler
blockiert den Gesamtscope, auch wenn ein alter Consumer-Bericht noch positiv
ist. `check()` ohne Ziel darf `ready=true` nur für `scope=connection`
ausweisen; Frontends dürfen daraus keine Freigabe einer Funktion ableiten.

Checks enthalten `name`, `status`, `required`, `source`, `observedAt` und
`expiresAt`. Issues enthalten `code`, `severity`, `publicMessage`, `action`
und Ziel-/Dienstbezug. Consumer-Zeilen enthalten Subscription, Service,
Instanz, Handler-Typ, Zustand und Frische. Messwerte ohne Messung sind null,
nicht null Sekunden oder null wartende Jobs. Stabile Codes sind unter
anderem `BROKER_UNREACHABLE`, `AUTHENTICATION_FAILED`, `AUTHORIZATION_DENIED`,
`HANDLER_NOT_REGISTERED`, `SCHEMA_UNAVAILABLE`, `DEPENDENCY_UNAVAILABLE`,
`OUTPUT_PERMISSION_DENIED`, `CONSUMER_NOT_RESPONDING`, `STATUS_STALE`,
`HEALTH_CHANNEL_UNAVAILABLE`, `INSUFFICIENT_READY_CONSUMERS`,
`EXPECTED_CONSUMERS_UNDEFINED`, `METRIC_UNAVAILABLE` und `BACKLOG_HIGH`.

Erwartete Betriebsprobleme werden im Report zurückgegeben, damit ein
Monitoringaufruf nicht beim ersten unerreichbaren Dienst abbricht. Ungültige
Check-Konfiguration wirft `InvalidHealthCheckException`; sonst bleibt die
Gesamtdeadline wirksam und unvollständige Teilprüfungen werden unknown.
Exceptions/OS-Fehler im Dienst werden gezielt auf Codes abgebildet, nicht
anhand beliebiger Fehlertexte erraten. Rohpfade, Zugangsdaten, Stacktraces
und interne Details gehören nicht in `publicMessage`.

### § 16.6 Frontend, Login und Überwachung

Beim Login kann das Backend einmal die für das Frontend benötigten
Topic-/Typ-Ziele prüfen und dem Browser pro Funktion `ready`, `status`,
`expiresAt` und freigegebene Fehlertexte liefern. Der Browser erhält keine
Broker-Credentials. Eine ausgefallene optionale Funktion muss nicht die
gesamte Anmeldung blockieren; die Anwendung legt fest, welche Funktionen
zwingend benötigt werden. Beispiel 09 zeigt eine solche Backend-Antwort.

Ein zentraler Monitor abonniert `system.health.v1`-Events, aktualisiert
die Sicht und meldet Zustandswechsel frühzeitig. Push verkürzt die
Erkennungszeit; periodische aktive Checks und Ablaufzeiten decken verlorene
Events oder verschwundene Dienste ab. Ein einmaliger Login-Check bleibt
nicht für die ganze Sitzung gültig: nach Ablauf wird erneut geprüft oder
eine frische, autorisierte Monitor-Sicht verwendet. Recovery-Meldungen
heben einen Fehler erst nach erfolgreicher Prüfung auf.

Der Collector dedupliziert generation/sequence pro Instanz und akzeptiert
keine Rückstufung auf eine ältere Sequenz. Eine neue Startgeneration wird
über eine aktuelle authentifizierte Probe oder vertrauenswürdige
Registrierung bestätigt, nicht anhand beliebiger verspäteter Events. Fremde
Probe-IDs, abgelaufene Berichte und ungültige Signaturen bleiben außerhalb
der aktuellen Bereitschaft. Ein einfaches `subscribe` allein ist noch kein
solcher vollständiger Collector; Beispiel 09 zeigt die Event-Anbindung.

Die Benachrichtigungsintegration dedupliziert gleiche Ursachen und kann
Hysterese/Backoff verwenden. Sie ist ein Adapter, keine eingebaute E-Mail-
oder Browser-Push-Plattform. Runtime-Fehlerbehandlung, Exceptions und
Idempotenz bleiben auch nach positivem Check erforderlich.

### § 16.7 Beispiel, Ausbaustufe und spätere Prüfungen

[Beispiel 09](../../examples/api-draft/09-system-check.php) enthält einen
Worker mit standardisierter Schreibrechte-Prüfung, aktiver Statusmeldung
und automatischem Pausieren/Wiederaufnehmen sowie gezielte Checks, eine
Frontend-Antwort und ein Monitor-Abonnement. Es ist wie alle Beispiele
ausschließlich API-Entwurf, keine bereitgestellte Health-Implementierung.

Erster Health-Ausbau: lokaler State, Verbindungsprobe, signierte Bereitschafts-
Probes, Instanz-/Gruppenaggregation, Push-/Recovery-Berichte und JSON-Vertrag.
Metrikadapter und externe Exporte sind optionale Erweiterungen. Vorgesehene
Tests: Broker erreichbar bei fehlendem Handler, Rechtefehler bei lebendem
Prozess, Status-Recovery, fehlende/falsche Instanz, Mindestkapazität versus
alle Teilnehmer, falsche Signatur, TTL/Sequenzen/Startgeneration, verlorene
Statusmeldung, blockierter Health-Kanal, hängender Probe-Callback und
Deadline-Verbrauch über mehrere Ziele. Keine fachlichen Nachrichten oder
automatischen Ressourcenänderungen durch einen Check.

Primärquelle: [RabbitMQ Monitoring](https://www.rabbitmq.com/docs/monitoring)
für die Unterscheidung von Broker-, Queue- und Anwendungszustand; abgerufen
am 2026-09-12. Der einheitliche API-/Statusvertrag ist der hier vorgeschlagene
Entwurf, kein behaupteter branchenweiter Standard.

### § 16.8 Deklarierte Abhängigkeiten und Listenerdiagnose

Die Anwendung erklärt in `HealthOptions::requirements`, welche Topic-/Typ-Paare
sie benötigt. `ReadinessRequirement(topic, type, subscriptions,
minReadyPerSubscription: 1)` beschreibt die erwarteten verarbeitenden Gruppen.
Eine eigene `subscribe`-Registrierung erklärt dagegen, was diese Anwendung
selbst empfängt; daraus wird keine Abhängigkeit von fremden Verarbeitern
erraten. Beide Richtungen bleiben im Statusbericht sichtbar.

Jede Consumer-Zeile enthält `subscription`, `serviceId`, `instanceId`, `topic`,
`type`, `status`, `ready`, `observedAt`, `expiresAt`, `issues` und optional
`diagnostics`. Der Runtime-Registry-Eintrag stammt aus tatsächlich registrierten
Handlern und der Consume-Bindung. Ein Listener kann also vorhanden, aber wegen
eines Rechtefehlers nicht bereit sein. Meldungen können nicht garantieren,
dass ein kurz danach abgestürzter Prozess noch vorhanden ist.

`presence` unterscheidet `present`, `absent`, `unknown`. `present` verlangt einen
frischen verifizierten Instanznachweis. `absent`/`NO_LISTENER` ist nur zulässig,
wenn eine autoritative aktuelle Registry/Connector-Sicht die Abwesenheit für
diesen Scope bestätigt. Ein fehlendes Probe-Reply bedeutet ansonsten
`unknown` mit `CONSUMER_NOT_RESPONDING`; ein alter Eintrag `STATUS_STALE`.
Auch die Aussage „kein Listener“ ist daher mit Quelle und Messzeit versehen.
Die Library darf nicht aus einem leeren Antwortarray Abwesenheit beweisen.

`HealthOptions::diagnostics` ist ein optionaler begrenzter Callback ohne
Parameter, der Betriebsdaten als Array liefert. Standardfelder sind `host`
(String oder null), `processMemoryBytes`, `processPeakMemoryBytes` und optional
`containerMemoryBytes` (je nichtnegative Integer oder null), mit gemeinsamer
`observedAt`-Zeit der Runtime. Prozesswerte im PHP-Beispiel messen den
PHP-Allocator, nicht RSS oder den ganzen Container. Containerwerte benötigen
einen eigenen passenden Adapter; Einheiten stehen im Feldnamen. Erweiterungen
verwenden einen anwendungseigenen Namensraum. Größenlimits und eine Allowlist
verhindern unbegrenzte Diagnose-Payloads. Ohne Provider fehlen die optionalen
Daten; das allein blockiert keine Bereitschaft.

Hostnamen und detaillierte Betriebsdaten sind für berechtigte interne
Überwachung opt-in, nicht automatisch Teil einer Browserantwort. Eine hohe
Speicherzahl ist zunächst eine Messung. Erst eine explizite Schwellenprüfung
setzt etwa einen degraded-Befund; eine überschrittene blockierende Grenze
muss als Pflichtbefund definiert sein. Die Library erfindet keine universell
passenden Speichergrenzen.

Beispiel einer Consumer-Zeile im standardisierten Bericht (synthetische Werte;
der vollständige Bericht hat zusätzlich die Felder aus § 16.5):

```json
{
  "subscription": "export-workers",
  "serviceId": "export-service",
  "instanceId": "export-2",
  "topic": "jobs.export",
  "type": "export.create.v1",
  "presence": "present",
  "status": "unhealthy",
  "ready": false,
  "observedAt": "2026-09-12T12:00:00Z",
  "expiresAt": "2026-09-12T12:00:20Z",
  "diagnostics": {
    "host": "worker-host-02",
    "processMemoryBytes": 33554432,
    "processPeakMemoryBytes": 41943040,
    "observedAt": "2026-09-12T12:00:00Z"
  },
  "issues": [{
    "code": "OUTPUT_PERMISSION_DENIED",
    "severity": "error",
    "publicMessage": "Der Exportdienst kann sein Ausgabeziel nicht beschreiben.",
    "action": "Berechtigungen des Ausgabeziels prüfen.",
    "serviceId": "export-service",
    "instanceId": "export-2"
  }]
}
```

Zusätzliche spätere Contract-Tests: deklarierte Mehrzielprüfung unter einem
Gesamtbudget, nachgewiesene Abwesenheit versus Timeout, vorhandener unbereiter
Listener, Dateneinheiten/null, Diagnose-ACL und Browser-Allowlist.
