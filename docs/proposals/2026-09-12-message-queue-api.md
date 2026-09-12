# Phore Message Queue: API- und Architekturentwurf

| Datum | Benutzername | Kurzbeschreibung |
|---|---|---|
| 2026-09-12 | dermatthes | §§ 1–12: Proposal mit API-Beispielen, Konnektorvergleich und Paketgrenzen angelegt |
| 2026-09-12 | dermatthes | §§ 1, 3, 5, 8, 11–14: Kleine explizite API, RPC, Begleitmeldungen, Metadaten und Middleware nach Frameworkvergleich ergänzt |
| 2026-09-12 | dermatthes | §§ 2, 12, 13.2, 15: Broadcast mit allen Lock-Antworten und Processing-Queue mit konkurrierenden Workern ergänzt |

## § 1 Abstract und Lieferumfang

Eine frameworkunabhängige PHP-Library stellt eine gemeinsame Zugriffsschicht
für Topics, dauerhafte Subscriptions und Worker bereit. Redis Streams ist der
erste produktive Konnektor. URL-Factory und direkte Konnektor-Injektion sind
gleichwertig. Message-Typen besitzen stabile fachliche Namen; ihre PHP-Klassen
dürfen sich zwischen Anwendungen unterscheiden. `phore/schema` validiert und
hydriert optional die lokal erwartete Struktur. PHP-Attribute ergänzen die
programmatische API. Signierung und Dateispeicher sind austauschbare Dienste.
Eine optionale Request/Reply-Schicht ergänzt RPC mit Rückgabewerten und
Begleitmeldungen; Metadaten und Middleware bleiben vom Payload getrennt.

**Dies ist ein Entwurf, keine implementierte oder installierbare API.** Das
Ziel-Repository enthält bisher nur die Projektvorlage, keine `src/`- oder
`test/`-Implementierung und keine eigene `SKILLS.md`. Als Namespace ist
`Phore\MessageQueue` vorgesehen; Composer-Name/Autoloading bleiben in diesem PR
unverändert. Beispiele verwenden PHP >=8.3, passend zur aktuellen Vorlage.

| Ausbaustufe | Geplanter Inhalt |
|---|---|
| Erste Umsetzung | Factory, Registry, JSON-Envelope, Topic/Subscription-API, Redis Streams, In-Memory, Callback-Worker, Ack/Retry/Dead Letter, Exceptions, HMAC, optionale Schema-Bridge und Attribute |
| Anschlussphase | Attachment-/PayloadStore-Vertrag mit lokalem Dateispeicher; separater Unix-Entwicklungsbroker mit Konnektor |
| Optionale RPC-Erweiterung | `request`/`respond`, Rückkanal, Ergebnis/Fehler/Warnings und Middleware aus §§ 13–14; baut auf der Queue-API auf |
| Weitere Adapter | SQS für Arbeitsqueues, SNS+SQS für Fan-out, Azure Service Bus, RabbitMQ |
| Spätere Erweiterungen | PGP-Provider, S3/Blob-PayloadStore, Batch, Delay, Filter, Replay, Telemetrie, optionale Outbox-/Inbox-Integration |

Die Beispiele illustrieren auch die Anschlussphase, ausdrücklich ohne sie in
diesem PR zu implementieren. Vor Umsetzung werden Konnektorabhängigkeiten und
unterstützte Serverversionen festgelegt; für Redis ist >=6.2 wegen `XAUTOCLAIM`
der vorgeschlagene Mindeststand. Provider-spezifische Erweiterungen dürfen
nicht stillschweigend auf schwächere Semantik zurückfallen.

### § 1.1 Kleine API auf einen Blick

Die Empfehlung ist eine einzige Queue-Fassade mit **fünf alltäglichen
Operationen**. Event, Request und Antwort-Handler sind am Verb erkennbar;
Broker, Routing, Schema und Middleware werden einmal konfiguriert.

```php
$mq->publish('users', 'user.created.v1', ['userId' => 'u-1']);
$mq->subscribe('users', 'billing-users', function (array $event): void { /* ... */ });

$reply = $mq->request('calculator', 'math.divide.v1', ['a' => 12, 'b' => 3])->await();
echo $reply->payload['quotient']; // 4; wartet ausdrücklich auf eine entfernte Antwort.

$mq->respond('calculator', 'calculator-workers', function (array $params): array {
    return ['quotient' => $params['a'] / $params['b']]; // Kurzform; vollständige Fehlerprüfung in Beispiel 05.
}, new SubscriptionOptions(type: 'math.divide.v1'));
$mq->run();
```

Diese Zeilen illustrieren getrennte Sender-/Empfängerprozesse, kein sequenziell
ausführbares Skript; der Responder muss vor dem Request laufen. Factory und
`close()` gehören zum Verbindungslebenszyklus. `emit($dto)` ist ausschließlich
der Komfortaufruf für `publish` mit Mapping; Attribute registrieren dieselben
Handler. Es gibt keine zweite RPC-Client-Fassade, kein eigenes Promise-Framework,
keinen Container-Zwang und kein mehrdeutiges `dispatch(..., true)`. Erweiterungen
kommen über Optionsobjekte und zwei Middleware-Hooks; Signierung, Codec und
Konnektoren sind Infrastruktur-Schnittstellen, keine Pflicht im täglichen Code.

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
Subscriptions. Ein Worker kann mehrere Topics abonnieren. „An alle“ bezeichnet
alle passenden benannten Subscriptions; „an einen“ einen ausgewählten Worker
innerhalb derselben Subscription. Die Subscription-Topologie bestimmt das
Verhalten, kein zusätzlicher Broadcast-Schalter beim Senden. Beispiele in § 15. [geändert]

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
| `MessageQueueInterface` | `publish`, `subscribe`, `request`, `respond`, `run`; Mapping-Komfort und Lebenszyklus gemäß § 1.1 |
| `MessageRegistry` | Fachliche Namen, Sendeklassen, optionale Schemas und Default-Topics zuordnen |
| `MessageCodecInterface` | JSON-kompatible Daten normalisieren, Envelope serialisieren und dekodieren |
| `SchemaMapperInterface` | Optional Strukturen prüfen und in lokal konfigurierte DTOs hydrieren |
| `MessageSecurityInterface` | Unveränderliche Nachrichtenbytes schützen und vor Verwendung verifizieren |
| `ConnectorInterface` | Bytes publizieren/empfangen, Receipt bestätigen/freigeben, Fähigkeiten melden |
| `PayloadStoreInterface` | Streams ablegen, Referenzen auflösen, Lebensdauer verwalten |
| `RetryPolicy` / `FailureStoreInterface` | Vorübergehende Fehler wiederholen, endgültige Fehler sicher ablegen |

Sendepfad: Typ/Topic auflösen → Send-Middleware ausführen → Daten normalisieren
und ggf. validieren → Dateien ablegen → Envelope kodieren → signieren → Größen-/Capability-Prüfung
→ Konnektor. Empfangspfad: begrenzten Transportframe lesen → Signatur und
Zeit-/Zielbindung prüfen → Envelope dekodieren → optional Dateien verifizieren
→ lokale Struktur prüfen/hydrieren → Handler-Middleware und Handler ausführen
→ bei RPC finale Antwort bestätigen lassen → Ack. Dateiinhalte werden erst
bei Zugriff geladen, bleiben aber vor Nutzung zu prüfen. Middleware darf weder
die Signaturprüfung noch Settlement umgehen; Details in § 14.3.

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
request(string $topic, string $type, array|object $params,
    ?RequestOptions $options = null): PendingReply;
respond(string $topic, string $subscription, callable $handler,
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
`correlationId`, getrennte `metadata` und Attachments tragen. Ein Retry eines
unklar bestätigten Publishes verwendet dieselbe ID und denselben fachlichen
Inhalt. `request`/`respond` sind die optionale RPC-Erweiterung aus § 13;
`subscribe` sendet niemals automatisch einen Rückgabewert.

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

RPC ergänzt `RequestTimeoutException`, `RemoteCommandException`,
`InvalidReplyException` und `RpcNotConfiguredException`. Die lokal vom
Responder geworfene `CommandFailedException` beschreibt einen ausdrücklich
freigegebenen fachlichen Fehler; über den Rückkanal geht nur dessen sicheres
Fehlerobjekt. Ein Fehlerlevel in einer Begleitmeldung ist kein terminaler
Command-Fehler und ändert die Settlement-Entscheidung nicht.

## § 12 Paketgrenzen, spätere Prüfungen und Quellen

In die Library gehören Transportvertrag, Registry, Worker-Lebenszyklus,
Serialization, optionale Schema-Bridge, Security-/PayloadStore-Schnittstellen
und konsistente Exceptions. Provider-SDKs werden über optionale Adapterpakete
eingebunden; welche davon als eigene Composer-Pakete erscheinen, wird bei
der Implementierungsplanung entschieden. SDK-Verträge lassen sich unabhängig
von Brokerinstallationen verteilen.

Nicht in den Kern gehören fachliche DTOs, Business-Workflows, vollständige
Job-Scheduler, langfristige Workflow-/RPC-Ergebnisarchive, Broker-Provisionierung über Cloud-IAM,
Admin-UIs, Virenscanner, ZIP-Entpackung, PGP-Keyverwaltung oder eine eigene
verteilte Dateispeicherplattform. Erweiterungspunkte dürfen diese verbinden,
ohne den Grundvertrag damit zu belasten. Keine scheinbar universellen
Transaktionen, Prioritäten oder Exactly-once-Zusagen. Der nun beauftragte
RPC-Umfang bleibt eine optionale Request/Reply-Erweiterung gemäß § 13.

Globale Lock-/Konsensverfahren gehören nicht in die MQ-Library. § 15 zeigt
Broadcast und das Einsammeln von Lock-Bestätigungen; die tatsächlichen
lokalen Leases und gegebenenfalls ein autoritatives Fencing-Verfahren
verantwortet ein separater Lock-Dienst der Anwendung. [neu]

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

## § 13 RPC: Command, Rückgabewert und Begleitmeldungen

[Beispiel 05](../../examples/api-draft/05-rpc.php) enthält Verbindung,
programmatischen Responder, alternativ denselben Handler per `#[Respond]`,
Parameterübergabe, Ergebnis, Warning und Fehlerbehandlung in getrennten
Prozessen. `request` veröffentlicht sofort und gibt `PendingReply` zurück;
erst `await()` blockiert. Der Responder liefert mit `return` ein Array oder
DTO. Ein skalarer Wert wird explizit als `['value' => ...]` verpackt.

### § 13.1 Einmalige Konfiguration und Aufruf

`ConnectionOptions::rpc` nimmt `RpcConnectionOptions` entgegen. Der Client
konfiguriert `replyTopic` und `replySubscription` einmal pro aktiver
Client-Instanz; der Server konfiguriert eine `allowedReplyTopics`-Allowlist.
Im lokalen Beispiel dürfen beide auf derselben HMAC-Audience arbeiten.
Anwendungen verwenden eigene Reply-Topics je Instanz, oder einen expliziten
zentralen Demultiplexer; konkurrierende Client-Prozesse dürfen nicht denselben
Reply-Consumer teilen und fremde Antworten wegkonsumieren. Die Rückkanal-
Subscription wird vor Veröffentlichung des ersten Requests bestätigt.

`RequestOptions` ergänzt `timeoutSeconds` (Default 30 Sekunden ab `request`,
nicht ab `await`), `metadata`, optional `responseClass` und `onNotice`.
`PendingReply::await(): Reply` verarbeitet nur den internen Rückkanal dieser
Connection, keine beliebigen Business-Handler. Mehrere Pending-Requests
teilen einen Dispatcher, der nach Request-ID puffert; Anzahl und Speicher
sind begrenzt. Gleichzeitige/nestende `run`-/`await`-Loops auf derselben
Connection sind ungültig. Für RPC aus einem Handler eine separate Connection
und einen unabhängig laufenden Responder verwenden.

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
| Error (`rpc.error.v1`) | Sicheres Fehlerobjekt mit `code`, `message`, begrenzten `details` | `requestId`, `kind=error`, eigene `messageId`, gesammelte Notices |
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

`request()->await()` wartet absichtlich nur auf eine terminale Antwort.
Wer Antworten aller Teilnehmer braucht, verwendet `publish` plus eine
aggregierende Subscription wie in § 15.2; hierfür wird keine mehrdeutige
`request(all: true)`-Option oder zusätzliche Queue-Methode eingeführt. [neu]

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

## § 14 Metadaten, Middleware und API-Entscheidung

[Beispiel 06](../../examples/api-draft/06-metadata-middleware.php) zeigt
Trace-/Locale-Metadaten, eine Send-Middleware, eine Handler-Middleware und
das eigenständige Publizieren von Warnungen/Fehlern auf ein Diagnose-Topic.
Das ist sowohl mit normalen Events als auch mit RPC nutzbar.

### § 14.1 Frameworkvergleich und API-Entscheidung

| Framework / Library | Recherchierter Ansatz | Entscheidung für diese API |
|---|---|---|
| Symfony Messenger | `dispatch`, Handler, Envelope/Stamps, Middleware; `HandledStamp` liefert Ergebnisse ausgeführter Handler, kein automatischer Remote-Rückkanal | Metadaten und Hooks übernehmen; Remote-Warten ausdrücklich `request()->await()` nennen |
| PHP Enqueue | `sendCommand` mit Reply-Option, Promise/`receive`, `Result::reply` und `ReplyExtension` | Request/Reply übernehmen; keine boolesche Option, die die Bedeutung eines normalen Sends verändert |
| RabbitMQ PHP-Tutorial | Callback-Queue, `reply_to`, `correlation_id`, Duplikatbehandlung | Rückkanal und IDs intern verwalten, nicht in jedem Handler manuell publizieren |
| NATS .NET Client | Explizites `RequestAsync`, Reply-Subject und Responder-Antwort | Verständliche Verben übernehmen; NATS-spezifische Inbox-Haltbarkeit nicht auf alle Broker übertragen |
| MassTransit | Typisierte Requests/Responses, Response-Address, Fault-Nachrichten und Timeouts | Sichere terminale Fehlerantwort und lokale Exception; zusätzliche Client-/Bus-Fabriken im Alltagsaufruf vermeiden |
| Laravel Queues | Job-Middleware um Handler-Ausführung mit Fortsetzungs-Callback | Kleinen Callable-Hook übernehmen, ohne Laravel-Job-Basisklasse und Container |

**Empfehlung für dieses Paket:** explizite Verben auf einer Queue-Instanz,
kleine Callbacks und optionale DTOs. Der alltägliche RPC-Aufruf benötigt nur
`request(...)->await()`, der Dienst nur `respond(...); run()`; das einmalige
Setup verwaltet Rückkanal und Policies. Zwei klare Methoden sind hier
verständlicher als ein `send` mit Mode-Flags oder ein generisches
Middleware-/Stamp-System für jeden einzelnen Aufruf. Das ist eine
Designabwägung für die beschriebenen Anforderungen, kein objektiver
Leistungsvergleich der Frameworks.

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
- [PHP Enqueue: Commands, Replies und Promise](https://php-enqueue.github.io/quick_tour/).
- [Symfony Messenger: Envelopes, Middleware und Handler-Ergebnisse](https://symfony.com/doc/current/messenger.html).
- [NATS .NET: Request/Reply und Queue-Gruppen](https://nats.io/blog/nats-dotnet-v2-alpha-release/).
- [MassTransit: Requests, Faults und Timeouts](https://masstransit.massient.com/concepts/requests).
- [Laravel 12: Job-Middleware](https://laravel.com/framework/docs/12.x/queues#job-middleware).

## § 15 An alle Subscriber oder an einen Worker

### § 15.1 Dasselbe Topic, bewusst gewählte Subscriptions

| Ziel | Subscription-Namen | Ergebnis |
|---|---|---|
| Alle beteiligten Dienste informieren | `locks-service-a`, `locks-service-b`, `locks-service-c` | Jeder Dienst erhält eine Kopie und kann separat antworten [neu] |
| Alle konkreten Instanzen informieren | Je Instanz ein stabiler eigener Name, etwa `locks-instance-17` | Jede erwartete Instanz erhält ihre eigene Kopie [neu] |
| Einen Job verteilen | Alle Worker: `text-processors` | Ein verfügbarer Consumer erhält die konkrete Zustellung zur Bearbeitung [neu] |

Ein Topic kann beides gleichzeitig haben, etwa eine Worker-Subscription und
eine unabhängige Audit-Subscription. „An einen“ bedeutet deshalb nicht
weltweit exklusiv, falls daneben weitere Subscriptions existieren. Für die
Processing-Queue provisioniert man bewusst nur die ausführende Worker-Gruppe;
Audit-Consumer führen den Job nicht aus. Die ersten beiden Muster brauchen
die Fan-out-Capability: Redis-Gruppen, RabbitMQ-Queues oder SNS+SQS passen,
ein einzelnes SQS-Queue-Backend kann nicht allen Gruppen Kopien liefern. [neu]

### § 15.2 Lock-Koordination: alle bekannten Teilnehmer antworten

[Beispiel 07](../../examples/api-draft/07-broadcast-locking.php) verwendet
`publish('maintenance.locks', 'lock.acquire.v1', ...)`. Jeder Teilnehmer
besitzt eine eigene Subscription auf diesem Topic, nimmt eine lokale Lease
für seine Ressource und sendet `lock.state.v1` an den Rückkanal. Der Koordinator
abonniert den Rückkanal vor dem Broadcast und zählt **Teilnehmer-IDs**, keine
Nachrichtenanzahl. Doppelte Antworten erhöhen den Zähler nicht. Erst alle
positiven Antworten der festen Teilnehmerliste erlauben den nächsten Schritt. [neu]

Die Liste ist ein Membership-Snapshot aus der Anwendungskonfiguration, keine
aus Queue-Subscriber-Zahlen erratene Größe. Erwartete Subscriptions werden
vor dem Lauf angelegt. Offline-Teilnehmer bleiben erwartet und führen zum
Timeout; neue Teilnehmer gehören erst zur nächsten Runde. Eine negative
Antwort oder die Akquise-Deadline bricht die Runde ab und löst einen
`lock.release.v1`-Broadcast aus. Jede Runde besitzt eine eindeutige ID;
alte/fremde Antworten werden verworfen. Je Rückkanal läuft nur ein
zuständiger Koordinator oder ein expliziter Demultiplexer. [neu]

Der gezeigte `LocalLeaseManager` ist eine **Anwendungsabhängigkeit**, keine
MQ-API. Erwerb ist idempotent pro Ressource/Runden-ID, hat eine absolute
begrenzte Gültigkeit und verlängert sich bei Redelivery nicht. Release gibt
nur die eigene Runde frei und hinterlässt bis zum Ablauf eine Abschlussmarke,
damit verspätete Acquire-Nachrichten einen freigegebenen Lock nicht erneut
nehmen. Leases laufen unabhängig vom Queue-Worker ab; dadurch bleiben bei
Koordinator-Crash oder verlorenem Release keine unbegrenzten Locks zurück. [neu]

„Alle haben ihren lokalen Lock bestätigt“ ist eine koordinierte Barriere,
kein Beweis eines linearisierbaren globalen Locks oder dauerhafter Gesundheit
aller Teilnehmer. Die kritische Arbeit muss vor der kleinsten sicheren
Lease-Deadline enden; Clock-Skew und Ausführungszeit brauchen Reserve. Ein
einfacher Zeitvergleich in PHP verhindert keine Pause nach dem Vergleich.
Für geschützte Schreibzugriffe muss die Zielressource deshalb veraltete
Operationen über einen autoritativen monotonen Fencing-Token ablehnen.
Die Runden-ID ist nur Korrelation/Ownership, kein solcher Fencing-Token.
Quorum/Konsens, Membership-Änderung und Lease-Verlängerung sind hier bewusst
keine Behauptung der Queue-Abstraktion. [neu]

Teilnehmer-IDs aus Reply-Payloads sind allein nicht vertrauenswürdig. Das
Beispiel setzt kooperative Teilnehmer mit gemeinsamer Entwicklungs-Identität
voraus. Produktion muss jede Antwort einer erlaubten Teilnehmeridentität
zuordnen, etwa über getrennte Signing-Keys/Principals oder getrennte
Reply-Topics mit durchgesetzten Publisher-ACLs. Ein gemeinsamer HMAC-Key
beweist nicht, welcher Teilnehmer tatsächlich den Lock besitzt. [neu]

### § 15.3 Processing-Queue: ein Worker verarbeitet und antwortet

[Beispiel 08](../../examples/api-draft/08-processing-workers.php) startet
mehrere Prozesse mit `respond('jobs.text', 'text-processors', ...)`.
**Der Subscription-Name bleibt bei allen Workern identisch.** Die Factory
erzeugt getrennte Transport-Consumer-IDs; eine Worker-ID dient im Beispiel
nur als Antwortmetadatum, nicht als neue Subscription. Der Client ruft
`request('jobs.text', 'text.process.v1', $params)->await()` auf und erhält
Payload und die Kennung des verarbeitenden Workers zurück. [neu]

Die Auswahl erfolgt brokerabhängig anhand verfügbarer Consumer, Credits,
Prefetch und Polling. „Random“ wird hier als „beliebiger verfügbarer Worker,
ohne feste Zielinstanz“ verstanden. Gleichmäßiger Zufall, Round-robin oder
garantierte Fairness sind kein portabler Vertrag; auch mehrere Jobs
hintereinander beim selben Worker sind zulässig. Wer eine bestimmte
Verteilungsstrategie benötigt, braucht einen gesonderten Scheduler. [neu]

Pro Zustellversuch wird ein Consumer ausgewählt; ein normaler Job wird
nicht an alle Worker kopiert. Bei Crash, verlorenem Ack oder Lease-Ablauf
kann derselbe Job dennoch erneut zugestellt werden. Ein pausierter alter
Worker kann nach Lease-Verlust sogar noch weiterlaufen, während ein neuer
übernimmt. „Nur ein Worker“ ist daher keine Exactly-once-/Seiteneffektgarantie:
lange Verarbeitung braucht Lease-Pflege, kritische Aktionen benötigen
Idempotenz oder ressourcenseitiges Fencing. Das Beispiel verarbeitet reinen
Text ohne externe Seiteneffekte; Ergebnis-Publish erfolgt gemäß § 13 vor
Request-Ack. [neu]

### § 15.4 Spätere Prüfungen und Quellen

Vorgesehene Contract-Tests: Broadcast an drei Subscriptions versus drei
Worker einer Gruppe, doppelte Teilnehmerantworten, fehlender/negativer
Teilnehmer, spätes Acquire nach Release, Koordinator-Crash, veraltete Lease,
falsche Teilnehmeridentität und erneute Job-Ausführung nach Lease-Verlust.
Diese Tests gehören zur späteren Implementierung, nicht zum Entwurfs-PR. [neu]

- [Redis XREADGROUP: Verteilung innerhalb von Consumer-Gruppen](https://redis.io/docs/latest/commands/xreadgroup/). [neu]
- [RabbitMQ Consumers: konkurrierende Consumer und Zustellsteuerung](https://www.rabbitmq.com/docs/consumers). [neu]
- [Redis: begrenzte Lock-Gültigkeit, Ownership und Fencing-Hinweise](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/). [neu]

Abruf: 2026-09-12; die konkrete API und die Barrierenlogik sind der
hier vorgeschlagene Anwendungsentwurf. [neu]
