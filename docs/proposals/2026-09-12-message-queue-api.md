# Phore Message Queue: API- und Architekturentwurf

| Datum | Benutzername | Kurzbeschreibung |
|---|---|---|
| 2026-09-12 | dermatthes | §§ 1–12: Proposal mit API-Beispielen, Konnektorvergleich und Paketgrenzen angelegt |

## § 1 Abstract und Lieferumfang

Eine frameworkunabhängige PHP-Library stellt eine gemeinsame Zugriffsschicht
für Topics, dauerhafte Subscriptions und Worker bereit. Redis Streams ist der
erste produktive Konnektor. URL-Factory und direkte Konnektor-Injektion sind
gleichwertig. Message-Typen besitzen stabile fachliche Namen; ihre PHP-Klassen
dürfen sich zwischen Anwendungen unterscheiden. `phore/schema` validiert und
hydriert optional die lokal erwartete Struktur. PHP-Attribute ergänzen die
programmatische API. Signierung und Dateispeicher sind austauschbare Dienste.

**Dies ist ein Entwurf, keine implementierte oder installierbare API.** Das
Ziel-Repository enthält bisher nur die Projektvorlage, keine `src/`- oder
`test/`-Implementierung und keine eigene `SKILLS.md`. Als Namespace ist
`Phore\MessageQueue` vorgesehen; Composer-Name/Autoloading bleiben in diesem PR
unverändert. Beispiele verwenden PHP >=8.3, passend zur aktuellen Vorlage.

| Ausbaustufe | Geplanter Inhalt |
|---|---|
| Erste Umsetzung | Factory, Registry, JSON-Envelope, Topic/Subscription-API, Redis Streams, In-Memory, Callback-Worker, Ack/Retry/Dead Letter, Exceptions, HMAC, optionale Schema-Bridge und Attribute |
| Anschlussphase | Attachment-/PayloadStore-Vertrag mit lokalem Dateispeicher; separater Unix-Entwicklungsbroker mit Konnektor |
| Weitere Adapter | SQS für Arbeitsqueues, SNS+SQS für Fan-out, Azure Service Bus, RabbitMQ |
| Spätere Erweiterungen | PGP-Provider, S3/Blob-PayloadStore, Batch, Delay, Filter, Replay, Telemetrie, optionale Outbox-/Inbox-Integration |

Die Beispiele illustrieren auch die Anschlussphase, ausdrücklich ohne sie in
diesem PR zu implementieren. Vor Umsetzung werden Konnektorabhängigkeiten und
unterstützte Serverversionen festgelegt; für Redis ist >=6.2 wegen `XAUTOCLAIM`
der vorgeschlagene Mindeststand. Provider-spezifische Erweiterungen dürfen
nicht stillschweigend auf schwächere Semantik zurückfallen.

## § 2 Begriffe und Zustellvertrag

| Begriff | Bedeutung und Beispiel |
|---|---|
| Topic | Logischer Nachrichtenkanal, etwa `users`; unabhängig vom Backendnamen |
| Message type | Fachlicher Vertrag, etwa `user.created.v1`; unabhängig von Namespace und Composer-Paket |
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
Subscriptions. Ein Worker kann mehrere Topics abonnieren.

Der Grundvertrag lautet **at least once innerhalb der konfigurierten
Aufbewahrung und Verfügbarkeit**. Doppelte Zustellungen sind möglich, ebenso
eine unklare Publish-Bestätigung bei Verbindungsabbruch. `PublishReceipt`
bestätigt Backend-Annahme, keine Verarbeitung durch Empfänger. Es gibt keine
backendübergreifende Exactly-once-Garantie und keine globale Reihenfolge.
Fachliche Seiteneffekte müssen anhand `messageId` idempotent sein.

`subscribe()` bindet eine benannte Subscription und prüft ihre Konfiguration.
Neue Subscriptions beginnen standardmäßig bei `StartPosition::Latest` zum
Zeitpunkt ihrer Anlage; bestehende behalten ihren Cursor. Ein späterer
Worker-Neustart setzt ihn niemals zurück. `Beginning` ist eine explizite
Replay-Capability und umfasst nur noch aufbewahrte Einträge. In Produktion
werden Topics und Subscriptions vorab provisioniert; nur eine ausdrücklich
aktivierte `autoCreate`-Option darf Ressourcen anlegen. `cancel()` löst die
lokale Bindung, löscht aber weder Subscription noch Rückstand.

## § 3 Abstraktionsschichten und Erweiterungspunkte

| Baustein | Verantwortung |
|---|---|
| `ConnectionFactory` / `ConnectionOptions` | DSN auswerten, installierten Adapter wählen, konfigurierte Dienste verbinden |
| `MessageQueueInterface` | `emit`, `publish`, `subscribe`, `registerHandlers`, `run`, `stop`, `close` |
| `MessageRegistry` | Fachliche Namen, Sendeklassen, optionale Schemas und Default-Topics zuordnen |
| `MessageCodecInterface` | JSON-kompatible Daten normalisieren, Envelope serialisieren und dekodieren |
| `SchemaMapperInterface` | Optional Strukturen prüfen und in lokal konfigurierte DTOs hydrieren |
| `MessageSecurityInterface` | Unveränderliche Nachrichtenbytes schützen und vor Verwendung verifizieren |
| `ConnectorInterface` | Bytes publizieren/empfangen, Receipt bestätigen/freigeben, Fähigkeiten melden |
| `PayloadStoreInterface` | Streams ablegen, Referenzen auflösen, Lebensdauer verwalten |
| `RetryPolicy` / `FailureStoreInterface` | Vorübergehende Fehler wiederholen, endgültige Fehler sicher ablegen |

Sendepfad: Typ/Topic auflösen → Daten normalisieren und ggf. validieren →
Dateien ablegen → Envelope kodieren → signieren → Größen-/Capability-Prüfung
→ Konnektor. Empfangspfad: begrenzten Transportframe lesen → Signatur und
Zeit-/Zielbindung prüfen → Envelope dekodieren → optional Dateien verifizieren
→ lokale Struktur prüfen/hydrieren → Handler ausführen → Ack.
Dateiinhalte werden erst bei Zugriff geladen, bleiben aber vor Nutzung zu prüfen.

Der Konnektor kennt keine Anwendungs-DTOnamen oder Callbacks. Seine
vorgeschlagenen primitiven Operationen sind `capabilities(): CapabilitySet`,
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

[Vollständige Beispiele: 01-connect.php](../../examples/api-draft/01-connect.php).
Der vorgeschlagene Einstieg lautet:

```php
$factory = new ConnectionFactory();
$mq = $factory->connect('redis://localhost:6379/0', $options);
$mq = $factory->fromConnector(new RedisStreamsConnector($redisConfig), $options);
$mq = $factory->fromAttributes(LocalConnection::class, $options);
```

Alle drei Methoden liefern `MessageQueueInterface`. `fromAttributes` liest
genau eine lokal angegebene Klasse mit `#[QueueConnection(dsn: ...)]`; kein
automatisches Scannen des Dateisystems. Die DSN-Auswertung verwendet eine
Schema-Allowlist und erzeugt niemals beliebige PHP-Klassen aus URL-Inhalten.
Provider können explizit über `registerConnectorFactory(scheme, factory)`
registriert werden. Unbekannte Schemes/Optionen werden abgelehnt.

| Vorgeschlagene DSN | Bedeutung |
|---|---|
| `redis://user:password@host:6379/0?prefix=app` | Redis Streams, ACL-Zugang; `/0` ist Datenbank |
| `rediss://user:password@host:6380/0` | Redis über TLS mit Zertifikatsprüfung |
| `redis://:password@host:6379/0` | Redis-Passwort ohne ACL-Benutzer |
| `redis+unix:///run/redis/redis.sock?db=0` | Redis-Server über Unix-Socket, weiterhin Redis-Protokoll |
| `memory://` | Isolierter In-Memory-Broker je Factory-Verbindung |
| `unix:///run/user/1000/phore-mq.sock` | Eigenes lokales MQ-Protokoll, benötigt separaten Dev-Broker |
| `sqs://eu-central-1/123456789012` | Geplanter Queue-Adapter; logische Topics per Routingtabelle auf Queue-URLs abbilden |
| `sns+sqs://eu-central-1/123456789012` | Geplanter Topic-Fan-out; SNS-ARNs und Subscription-Queues aus Routingtabelle |
| `azure-servicebus://namespace.servicebus.windows.net` | Geplanter Service-Bus-Adapter mit Topic-/Subscription-Bindings |
| `amqp://user:password@host:5672/vhost` | Geplanter RabbitMQ-Adapter; TLS über `amqps` |

Diese Schemes sind Library-Konventionen, keine Zusage bereits vorhandener
Treiber. Benutzername, Passwort und Token vor `@` werden einmal percent-dekodiert;
`@` im Passwort muss `%40` sein. Port, IPv6, Pfad, doppelte Query-Parameter
und Optionswerte werden strikt geprüft. Fehler/Logs redigieren Credentials.
Ein einzelner Key lässt sich für passende Anbieter als Passwort transportieren;
Cloud-Adapter bevorzugen explizit injizierte Credential-Provider für temporäre
Tokens und Managed Identity. Kein implizites Lesen von Environment-Variablen.
Broker-Zugangsdaten und HMAC-Shared-Secret sind getrennte Einstellungen.

Attribute enthalten höchstens lokale Beispiel-DSNs oder Verbindungsnamen,
keine produktiven Secrets. Für produktive Deployment-Konfiguration ist die
programmatische Factory vorzuziehen.

## § 5 Senden, empfangen und Worker-Lebenszyklus

[Programmatische Beispiele: 02-programmatic.php](../../examples/api-draft/02-programmatic.php).
Die vorgeschlagenen öffentlichen Signaturen lauten:

```php
publish(string $topic, string $type, array|object $payload,
    ?PublishOptions $options = null): PublishReceipt;
emit(object $message, ?PublishOptions $options = null): PublishReceipt;
subscribe(string $topic, string $subscription, callable $handler,
    ?SubscriptionOptions $options = null): SubscriptionHandle;
registerHandlers(object $handler): void;
run(?RunOptions $options = null): void;
stop(): void;
close(): void;
```

`publish` benennt Topic und Typ ausdrücklich; `emit` liest sie aus Registry
oder Attribut der lokalen Sendeklasse. Ein fehlendes oder widersprüchliches
Mapping wirft `MessageMappingException`. Explizites Mapping hat Vorrang vor
Attributen; mehrfache programmatische Registrierung desselben Sendetyps wird
abgelehnt. `PublishOptions` kann eine stabile `messageId`, `expiresAt`,
`correlationId` und Attachments tragen. Ein Retry eines unklar bestätigten
Publishes verwendet dieselbe ID und denselben fachlichen Inhalt.

`SubscriptionOptions` enthält optional `type` als exakten Filter,
`payloadClass` als lokale Zielklasse, `startAt`, `ackMode`, `retryPolicy` und
`durability` (Default `Durability::Durable`). Memory/Unix-Tests wählen explizit
`Durability::Volatile`; damit wird keine Haltbarkeit über Prozessneustarts
versprochen. Fehlende angeforderte Haltbarkeit ist ein Capability-Fehler.
Ohne Typfilter muss der Array-Handler alle
Nachrichtentypen des Topics verarbeiten können. Nicht passende Typen werden
für diese Subscription bewusst übersprungen und bestätigt; ein separater
Handler darf nicht dieselbe Subscription mit anderem Filter übernehmen.
Filteränderungen benötigen eine neue Subscription oder explizite Migration.

`subscribe` registriert und bindet, `run` startet den blockierenden Empfang.
Vorgesehen: `RunOptions(maxMessages, maxSeconds, idleTimeoutSeconds)`;
`run` kehrt beim ersten erreichten Limit zurück. `stop` beendet nach dem
laufenden Handler, `close` gibt Verbindungen frei. Empfangs-Timeout ohne
Nachricht ist kein Fehler. Ein Handler erhält Payload und optional
`MessageContext`; letzterer liefert `messageId`, Typ, Topic, Versuch und
Attachment-Zugriff. Broker-spezifische Objekte werden nicht weitergereicht.

Standardmäßig folgt Ack erst nach erfolgreicher Callback-Rückkehr. Ein
temporärer Handlerfehler löst eine begrenzte Retry-Policy aus; endgültige
Fehler gehen vor Bestätigung in den FailureStore. `AckMode::Manual` erlaubt
`context->ack()`, `context->retry(delaySeconds: ...)` oder
`context->reject(reason: ...)`. Es ist genau eine Settlement-Entscheidung pro
Zustellung zulässig. Rückkehr ohne Settlement gibt die Nachricht erneut frei.
`context->extendLease(seconds: ...)` ist capabilityabhängig; lange synchrone
Handler müssen aktiv verlängern oder eine ausreichende Lease konfigurieren.

Retry kann durch native Redelivery/Visibility oder Adapterlogik erfolgen.
Es darf keine verlustbehaftete Folge aus Ack vor erneutem Publish geben.
Nichtatomare Kopier-vor-Ack-Schritte dürfen Duplikate erzeugen und müssen
dies dokumentieren. Fehler im FailureStore führen zu **keinem Ack** und
beenden den Worker mit Infrastrukturfehler. Ein Error-Observer bekommt
sanitisierte Fehlerdaten; systemische Transportfehler werden aus `run`
geworfen statt in einer Endlosschleife verborgen.

## § 6 SDK-Typen, Attribute und strukturelle Kompatibilität

[Attributbeispiele: 03-attributes.php](../../examples/api-draft/03-attributes.php).
Mit „Annotationen“ sind zunächst native PHP-8-Attribute gemeint:
`#[MessageType('user.created.v1', topic: 'users')]` auf DTOs und
`#[Subscribe(topic: 'users', subscription: 'billing-users',
type: 'user.created.v1')]` auf öffentlichen Methoden. PHPDoc-Annotationen als
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

## § 7 Redis-Standard und Konnektorvergleich

Die folgende Bewertung ist eine Designableitung aus den verlinkten
Primärquellen, keine Aussage über bereits implementierte Adapter.

| Kandidat | Relevante Fähigkeiten | Konsequenz für dieses Paket |
|---|---|---|
| Redis Streams | Log, Consumer Groups, Pending-Liste, Ack, Claim verwaister Nachrichten | Standard: Stream pro Topic, Gruppe pro Subscription, eindeutiger Consumer pro Worker |
| Redis Pub/Sub | Flüchtige Broadcasts und Patterns; at most once | Optionaler eigener Modus, kein Ersatz für dauerhafte Subscriptions |
| Amazon SQS | Arbeitsqueue; Consumer teilen Nachrichten | `sqs` meldet nur konkurrierende Queue-Verarbeitung; zweite unabhängige Fan-out-Subscription wird abgelehnt |
| Amazon SNS + SQS | Topic-Fan-out in getrennte Queues | Vollständiges Subscription-Modell über SNS-Topic und Queue je Subscription |
| Azure Service Bus | Queues, Topics, dauerhafte Subscriptions und Filter | Geeigneter Cloud-Adapter; Credential-/PHP-Client-Auswahl noch prüfen |
| RabbitMQ | Exchanges/Bindings, Queues, Consumer-Ack und Publisher Confirms | Topic auf Exchange, Subscription auf Queue; AMQP-Protokollversion ausdrücklich festlegen |
| NATS JetStream | Persistente Streams, langlebige Consumer, Ack und Redelivery | Späterer Adapter, Core NATS nicht mit JetStream gleichsetzen |
| In-Memory | Prozessinterne kontrollierte Zustellung | Frühes Testwerkzeug, kein Ersatz für Brokerintegrationstests |
| Unix-Socket | Lokaler Byte-Transport | Benötigt Dev-Broker für Routing, Gruppen und Receipts; keine Queue allein durch Socket/Semaphore |

Redis benötigt getrennte Empfangs-/Publish-Verbindungen, begrenztes Blocking
und eindeutige Consumer-IDs. `XREADGROUP` liefert neue Nachrichten; Pending-
Recovery über `XAUTOCLAIM` und Ack über `XACK`. Ein Ack darf den Stream-Eintrag
nicht global löschen, solange andere Subscriptions ihn brauchen.
Aufbewahrungsregeln berücksichtigen langsame Gruppen und Pending-Einträge;
aggressives `MAXLEN` kann noch benötigte Daten entfernen. Redis-Persistenz,
Replikation und Eviction-Policy sind Betriebsentscheidungen und bestimmen
die tatsächliche Haltbarkeit. Der Adapter muss verlorene/ge-trimmte Pending-
Einträge sichtbar melden und darf sie nicht als erfolgreich verarbeitet werten.

`capabilities()` beschreibt mindestens durableSubscriptions, competingConsumers,
acknowledgements, retry, deadLetter, leaseExtension, replay, delayedPublish,
ordering, filtering und maxFrameBytes. Zusätzliche Optionen werden nur bei
Unterstützung akzeptiert; etwa Delay, Priorität, FIFO und Transaktionen sind
keine universellen Versprechen. Transportgrößen werden inklusive Envelope,
Signatur, Encoding und Anbieter-Metadaten bewertet, nicht allein am Payload.

### § 7.1 Was andere PHP-Abstraktionen bereits vorsehen

Symfony Messenger zeigt DSN-Transports, Handler-Attribute, Envelopes/Middleware,
Retry/Failure-Transports, Worker-Limits, In-Memory-Tests und optionale
Message-Signierung. PHP Enqueue zeigt Connection-Factory, Context,
Producer/Consumer und explizite Acknowledgements. Daraus übernehmen wir eine
kleine öffentliche API, separate Transportverträge und einen klaren
Fehler-/Worker-Lebenszyklus. Das Paket wird dadurch kein Framework und
benötigt weder Symfony-Servicecontainer noch automatische Handler-Suche.

## § 8 Transparente Sicherheit

Default-Provider bei konfiguriertem Shared Secret ist **HMAC-SHA-256** mit
Key-ID. Ein bloßer SHA-Hash mit angehängtem Secret ist kein geeignetes
Signaturverfahren. Verbindungskonfiguration verlangt eine explizite Policy:
HMAC oder bewusstes `UnsignedSecurity` für isolierte Tests; kein generiertes,
fest eingebautes oder stillschweigend fehlendes Secret. Ein Empfänger mit
HMAC-Policy weist unsignierte Nachrichten immer zurück.

Das signierte Envelope enthält Protokollversion, `messageId`, fachlichen Typ,
Topic, Audience, UTC-`issuedAt`, optional `expiresAt`, Content-Type, Payload,
Correlation-Metadaten und Attachment-Deskriptoren inklusive Digest/Länge.
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
Queue-Backlog, Retry- und Replay-Fenster passen; kein pauschales Fünf-Minuten-
Limit für dauerhafte Queues. Redelivery behält ID, Bytes und ursprüngliche
Signatur. Broker-Versuchszähler gehören nicht zum unveränderlichen Envelope.

Signierung verhindert Replay allein nicht. Eine optionale Inbox speichert
`(audience, subscription, messageId)` mit Zuständen processing/completed und
begrenzten Leases. Erst erfolgreicher Abschluss markiert completed;
fehlgeschlagene Versuche dürfen erneut verarbeitet werden. Ein früher globaler
Nonce-Verbrauch würde legitime Wiederholungen und andere Subscriptions
blockieren. Atomizität zwischen fachlicher DB-Änderung und Inbox erfordert
Anwendungs-/Transaktionsintegration, nicht nur Redis-Deduplication.

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
`PayloadStoreInterface` übernimmt Upload und spätere Auflösung. Dieses
Claim-Check-Verfahren ist auch bei AWS/Azure beschrieben. Binärdaten werden
nicht unbeschränkt base64-kodiert in Redis/SQS geschoben.

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

Der lokale FileStore funktioniert nur bei gemeinsam zugänglichem Dateisystem;
für mehrere Hosts braucht es etwa S3 oder Azure Blob. Begrenzungen gelten
für Dateigröße, Zahl der Attachments, Downloads und temporären Speicher.
Automatisches Offloading beliebig großer JSON-Bodies sowie Chunking mit
Reassembly sind spätere Erweiterungen und kein impliziter Bestandteil von
`publish`. Ohne Store oder bei zu großem Frame folgt eine eindeutige Exception.

## § 10 Lokale Entwicklung: Memory, Redis-Socket und Dev-Broker

`memory://` durchläuft denselben Codec, dieselbe Signierung und dieselbe
Schema-Bridge. Es kopiert serialisierte Nachrichten, keine veränderbaren
Objektreferenzen. Zwei unabhängig erstellte Memory-Verbindungen teilen keinen
Broker; für Sender/Empfänger innerhalb eines Tests wird derselbe explizite
`InMemoryBroker` an zwei Konnektoren injiziert. Deterministische Clock und
kontrollierte Redelivery sind nützliche spätere Test-Hooks.

`redis+unix://` ist die einfache lokale Variante mit echter Redis-Semantik.
`unix://` ist dagegen ein eigener Konnektor: Ein separat gestarteter
`UnixDevBroker` verwaltet Topics, Subscriptions, konkurrierende Consumer und
volatile Pending-Receipts. Vorgeschlagenes Protokoll: begrenzte längenpräfixierte
Frames, Version, Request-ID, Publish, Subscribe, Delivery, Ack und Release;
partielle Reads/Writes, Backpressure und Disconnect müssen behandelt werden.
Nach Disconnect wird nicht bestätigte Arbeit erneut angeboten, solange der
Broker lebt; nach Broker-Neustart ist dessen Arbeitsspeicher verloren.

Eine Semaphore koordiniert Zugriffe oder signalisiert Zustände, speichert
aber weder Nachrichten noch Abonnements. Sie ist höchstens ein internes
Hilfsmittel. Socketdatei und Elternverzeichnis brauchen passende Zugriffsrechte;
kein weltbeschreibbarer gemeinsamer Pfad, kein Überschreiben fremder Sockets.
Windows-Unterstützung, persistentes Spooling, Clustering und ein eigener
produktiver Broker gehören nicht zur ersten lokalen Implementierung.

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
| `UnsupportedConnectorException` / `MissingDependencyException` | Treiber oder Schema-Bridge fehlt; vor Workerstart abbrechen |
| `UnsupportedCapabilityException` | Dauerhafter Fan-out mit reinem SQS oder Replay ohne Unterstützung |
| `ConnectionException` / `AuthenticationException` | Netzwerkproblem retrybar; falsche Credentials nicht endlos wiederholen |
| `PublishException` | Annahme fehlgeschlagen oder unbekannt; `outcome` = rejected/unknown |
| `MessageMappingException` / `InvalidHandlerException` | Fehlender Typname, Konflikt oder mehrdeutige Reflection |
| `SerializationException` / `InvalidEnvelopeException` | Nicht unterstützte Payload oder defekter Frame; endgültig |
| `MessageValidationException` | `user.created.v1: $.email: required property is missing` |
| `MessageHydrationException` | Konstruktor-/Property-Zuweisung gescheitert; Schema-Exception als previous |
| `InvalidSignatureException` / `ExpiredMessageException` | Keine Verarbeitung; Security-Failure-Policy |
| `PayloadTooLargeException` / `PayloadStoreRequiredException` | Brokergrenze überschritten oder Dateispeicher fehlt |
| `AttachmentUnavailableException` / `AttachmentIntegrityException` | Storefehler ggf. retrybar; falscher Digest endgültig |
| `RetryableMessageException` / `RejectMessageException` | Explizite fachliche Wiederholung bzw. endgültige Ablehnung |
| `SettlementException` / `LeaseLostException` | Ack fehlgeschlagen/Lease verloren; Duplikate berücksichtigen |
| `FailureStoreException` | Sichere Fehlerablage fehlgeschlagen; kein Ack, Worker abbrechen |

Validierung meldet konkrete Pfade und erwartete Typen, aber keine sensiblen
Istwerte. Ein unbekannter Typ wird bei explizitem Filter übersprungen;
trifft er einen Handler, der ein registriertes Schema verlangt, ist dies ein
Mappingfehler. Nicht explizit klassifizierte Handler-Exceptions werden
begrenzt wiederholt und anschließend abgelegt. Syntax-/Konfigurationsfehler
sind keine Nachrichten-Retries.

## § 12 Paketgrenzen, spätere Prüfungen und Quellen

In die Library gehören Transportvertrag, Registry, Worker-Lebenszyklus,
Serialization, optionale Schema-Bridge, Security-/PayloadStore-Schnittstellen
und konsistente Exceptions. Provider-SDKs werden über optionale Adapterpakete
eingebunden; welche davon als eigene Composer-Pakete erscheinen, wird bei
der Implementierungsplanung entschieden. SDK-Verträge lassen sich unabhängig
von Brokerinstallationen verteilen.

Nicht in den Kern gehören fachliche DTOs, Business-Workflows, vollständige
Job-Scheduler, RPC-Ergebnisverwaltung, Broker-Provisionierung über Cloud-IAM,
Admin-UIs, Virenscanner, ZIP-Entpackung, PGP-Keyverwaltung oder eine eigene
verteilte Dateispeicherplattform. Erweiterungspunkte dürfen diese verbinden,
ohne den Grundvertrag damit zu belasten. Keine scheinbar universellen
Transaktionen, Prioritäten oder Exactly-once-Zusagen.

Für die spätere Umsetzung sind fokussierte Contract-Tests vorgesehen:
unabhängige Subscriptions versus Worker-Gruppe, Redelivery nach Crash,
Ack-Verlust, unbekanntes Publish-Ergebnis, lokale DTOs mit anderem Namespace,
verschachtelte Strukturen/required/null/zusätzliche Felder, manipulierte
Signaturen samt Metadaten, Rotation, Backlog-Zeitprüfung, fehlgeschlagene
Dateiprüfung, konkurrierende Consumer und Socket-Teilverarbeitung. Dieser
Entwurfs-PR fügt keine Laufzeitimplementierung oder Tests dafür hinzu.

Primärquellen, abgerufen am 2026-09-12:

- §§ 1, 7: [Redis Pub/Sub und Zustellgarantien](https://redis.io/docs/latest/develop/pubsub/), [XREADGROUP](https://redis.io/docs/latest/commands/xreadgroup/), [XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/).
- §§ 3, 5, 7.1, 8: [Symfony Messenger: Transports, Retry, Attribute und Signierung](https://symfony.com/doc/current/messenger.html), [PHP Enqueue Quick Tour](https://php-enqueue.github.io/quick_tour/).
- § 7: [SNS-Fan-out an SQS](https://docs.aws.amazon.com/sns/latest/dg/sns-sqs-as-subscriber.html), [Azure Service Bus: Queues, Topics, Subscriptions](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-queues-topics-subscriptions).
- § 7: [RabbitMQ Exchanges](https://www.rabbitmq.com/docs/exchanges), [Acknowledgements und Publisher Confirms](https://www.rabbitmq.com/docs/confirms), [NATS JetStream Consumers](https://docs.nats.io/learn/jetstream/pull-consumers).
- § 9: [AWS SQS Extended Client und S3](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-managing-large-messages.html), [Azure Claim-Check Pattern](https://learn.microsoft.com/en-us/azure/architecture/patterns/claim-check).
- § 10: [PHP stream_socket_server](https://www.php.net/manual/en/function.stream-socket-server.php).
- § 6.1: [phore/schema Hydrator](https://github.com/phore/phore-schema/blob/aa8e60ab3b371fc3503f2a7ec8e2a3a63305074c/src/Hydrator/Hydrator.php), [Validator](https://github.com/phore/phore-schema/blob/aa8e60ab3b371fc3503f2a7ec8e2a3a63305074c/src/Validator/Validator.php), [Nutzungsinfo](https://github.com/phore/phore-schema/blob/aa8e60ab3b371fc3503f2a7ec8e2a3a63305074c/.ai-usage-info.md).
