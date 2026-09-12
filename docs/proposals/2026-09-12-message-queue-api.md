# Phore Message Queue: API- und Architekturentwurf

| Datum | Benutzername | Kurzbeschreibung |
|---|---|---|
| 2026-09-12 | dermatthes | §§ 1–12: Proposal mit API-Beispielen, Konnektorvergleich und Paketgrenzen angelegt |
| 2026-09-12 | dermatthes | §§ 1, 3, 5, 8, 11–14: Kleine explizite API, RPC, Begleitmeldungen, Metadaten und Middleware nach Frameworkvergleich ergänzt |
| 2026-09-12 | dermatthes | §§ 2, 12, 13.2, 15: Broadcast mit allen Lock-Antworten und Processing-Queue mit konkurrierenden Workern ergänzt |
| 2026-09-12 | dermatthes | §§ 1.1, 16: Standardisierte Systemchecks, deklarierte Nachrichtenabhängigkeiten, Listenerdiagnose und Frontend-/Monitoring-Anbindung ergänzt |
| 2026-09-12 | dermatthes | §§ 1, 1.1, 3, 4: PhoreMQ als zentrales Objekt mit DSN-/Connector-Konstruktor und gleichwertiger Factory-Erzeugung ergänzt |
| 2026-09-12 | dermatthes | §§ 5, 6, 6.2, 11: Callback-Kurzform, abgeleitete Metadaten, offene Topics und frühe Konfliktprüfung ergänzt |

## § 1 Abstract und Lieferumfang

Eine frameworkunabhängige PHP-Library stellt eine gemeinsame Zugriffsschicht
für Topics, dauerhafte Subscriptions und Worker bereit. Redis Streams ist der
erste produktive Konnektor. Das zentrale Objekt `PhoreMQ` wird direkt mit
DSN oder Konnektor und `ConnectionOptions` erzeugt; alternativ liefert die
Connection-Factory dasselbe Objekt. Message-Typen besitzen stabile fachliche Namen; ihre PHP-Klassen
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

Die Empfehlung ist die konkrete Queue-Fassade `PhoreMQ`, die
`MessageQueueInterface` implementiert, mit **fünf alltäglichen Operationen**.
Event, Request und Antwort-Handler sind am Verb erkennbar; Broker, Routing,
Schema und Middleware werden einmal am Objekt konfiguriert.

```php
$mq = new PhoreMQ($dsn, $options); // Einmal erzeugen; DSN oder Connector.
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
ausführbares Skript; der Responder muss vor dem Request laufen. Konstruktor
bzw. Factory und `close()` gehören zum Verbindungslebenszyklus. `emit($dto)` ist ausschließlich
der Komfortaufruf für `publish` mit Mapping; Attribute registrieren dieselben
Handler. Es gibt keine zweite RPC-Client-Fassade, kein eigenes Promise-Framework,
keinen Container-Zwang und kein mehrdeutiges `dispatch(..., true)`. Erweiterungen
kommen über Optionsobjekte und zwei Middleware-Hooks; Signierung, Codec und
Konnektoren sind Infrastruktur-Schnittstellen, keine Pflicht im täglichen Code.

Für Diagnose gibt es zusätzlich genau einen Queue-Aufruf `check()`.
Dienstentwickler melden Zustandsänderungen über `HealthState::set()` in einem
gemeinsamen lokalen Zustandsobjekt; Transport, aktive Meldung und Ping-Antwort
verwaltet die Library. Details und standardisierter Vertrag in § 16.


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
Verhalten, kein zusätzlicher Broadcast-Schalter beim Senden. Beispiele in § 15.

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
| `ConnectionFactory` / `ConnectionOptions` | Alternative Erzeugung von `PhoreMQ` und gemeinsame Konfiguration; dieselbe DSN-Auflösung wie im Konstruktor |
| `PhoreMQ` | Zentrales Objekt; akzeptiert DSN oder Connector und Optionen, implementiert `MessageQueueInterface` und verwaltet den Lebenszyklus |
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
Der normale Einstieg erzeugt unmittelbar das zentrale Objekt; die Varianten
sind Alternativen, nicht mehrere benötigte Verbindungen:

```php
use Phore\MessageQueue\PhoreMQ;

$mq = new PhoreMQ('redis://localhost:6379/0', $options);
// Oder einen bereits konfigurierten Connector injizieren:
$mq = new PhoreMQ($connector, $options);
// Auch mit benannten Argumenten:
$mq = new PhoreMQ(connection: $dsn, options: $options);

// Gleichwertige Alternative, etwa im DI-Bootstrap:
$factory = new ConnectionFactory();
$mq = $factory->connect($dsn, $options);                   // PhoreMQ
$mq = $factory->fromConnector($connector, $options);       // PhoreMQ
$mq = $factory->fromAttributes(LocalConnection::class, $options); // PhoreMQ
```

Vorgeschlagene öffentliche Erzeugungssignaturen (Deklarationsauszug,
keine Implementierung):

```php
// Phore\MessageQueue\PhoreMQ implements MessageQueueInterface
public function __construct(
    string|ConnectorInterface $connection,
    ?ConnectionOptions $options = null,
);

// ConnectionFactory
public function connect(string $dsn, ?ConnectionOptions $options = null): PhoreMQ;
public function fromConnector(ConnectorInterface $connector, ?ConnectionOptions $options = null): PhoreMQ;
public function fromAttributes(string $class, ?ConnectionOptions $options = null): PhoreMQ;
```

`ConnectorInterface` liegt unter `Phore\MessageQueue\ConnectorInterface`.
Es gibt genau eine Verbindungsangabe: String bedeutet DSN, ein Objekt muss
das Connector-Interface implementieren. Host, Port und Broker-Credentials
kommen aus DSN oder Connector-Konfiguration; Schema, Security, Routing,
Middleware, RPC, Health und Dateispeicher aus `ConnectionOptions`. Sämtliche
Einstellungen werden damit beim Erzeugen übergeben. Es gibt keine parallelen
DSN-/Connector-Felder im Optionsobjekt, keine später notwendigen Setter und
kein zusätzliches `connect()` auf dem MQ-Objekt.

`null` bedeutet ein frisches Optionsobjekt mit denselben dokumentierten
Defaults für alle Erzeugungswege, kein implizites Lesen von Environment oder
Secrets. Erforderliche Security-/Provider-Konfiguration muss weiterhin
explizit vorliegen; fehlende Konfiguration wird nicht durch unsichere Defaults
ersetzt. Konfiguration wird beim Erzeugen validiert und als Snapshot verwendet;
spätere Mutation des Optionsobjekts ändert das laufende MQ nicht. Explizit
zustandsbehaftete injizierte Dienste wie `HealthState` bleiben dagegen geteilt.

Konstruktor und Factory bauen die Verbindung sofort mit begrenztem
Verbindungstimeout auf. Erfolgreiche Rückkehr liefert ein verwendbares
`PhoreMQ`; sie bestätigt noch keine fremden Listener oder nachrichtenspezifische
Bereitschaft (dafür `check`). Beide Wege werfen dieselben Konfigurations-,
DSN-, Verbindungs- und Auth-Exceptions aus § 11. Teilweise geöffnete eigene
Ressourcen werden bei einem Fehler freigegeben. Kein verstecktes Lazy-Connect
mit erst beim ersten Publish auftretendem initialem Verbindungsfehler.

Eine interne gemeinsame Initialisierung löst DSNs auf, validiert Optionen
und bindet Connector und Dienste genau einmal. Die Factory delegiert an
diesen Erzeugungsweg; der Konstruktor ruft nicht rekursiv die öffentliche
Factory auf. Direkte Connector-Injektion umgeht ausschließlich die DSN-
Auflösung, niemals Security, Codec, Middleware oder Capability-Prüfungen.
Die Factory gibt das `PhoreMQ` selbst zurück, keinen zusätzlichen Wrapper.
Anwendungscode kann für austauschbare Abhängigkeiten weiterhin gegen
`MessageQueueInterface` typisieren.

Ein MQ-Objekt wird einmal je Verbindung und Prozess erzeugt und für alle
zugehörigen Topics, Registrierungen, RPC und Checks wiederverwendet; kein
globaler Singleton. `close()` ist idempotent und schließt die zugehörigen
Transportressourcen, `stop()` beendet nur den Worker-Loop. Ein an `PhoreMQ`
übergebener Connector steht exklusiv unter dessen Lebenszyklusverwaltung,
auch beim gescheiterten Aufbau; er darf nicht gleichzeitig in ein zweites
MQ-Objekt injiziert werden. Für geteilte In-Memory-Daten erhält jedes MQ einen
eigenen Connector am selben `InMemoryBroker`. Separate RPC-/Health-Verbindungen
bleiben bei den in §§ 13 und 16 beschriebenen Laufzeitanforderungen nötig.

`fromAttributes` liest genau eine lokal angegebene Klasse mit
`#[QueueConnection(dsn: ...)]`; kein automatisches Scannen des Dateisystems.
Die gemeinsame DSN-Auswertung verwendet eine Schema-Allowlist und erzeugt
niemals beliebige PHP-Klassen aus URL-Inhalten. Eigene Provider können lokal
an der Factory über `registerConnectorFactory(scheme, factory)` registriert
werden. Diese Registrierung verändert keine globale Registry: der einfache
Konstruktor kennt nur die freigegebenen Standard-Schemes; für eigene Schemes
nutzt man die konfigurierte Factory oder injiziert den Connector direkt.
Unbekannte Schemes/Optionen werden in beiden Wegen abgelehnt.

Vorgesehene spätere Contract-Tests: gleicher konkreter Rückgabetyp und
Funktionsumfang, gleiche Defaults/Exceptions/Sicherheitskette, einmaliger
Verbindungsaufbau, Ressourcenfreigabe bei Teilfehlern, exklusives Connector-
Ownership und idempotentes `close`. In diesem PR bleibt dies API-Entwurf.

| Vorgeschlagene DSN | Bedeutung |
|---|---|
| `redis://user:password@host:6379/0?prefix=app` | Redis Streams, ACL-Zugang; `/0` ist Datenbank |
| `rediss://user:password@host:6380/0` | Redis über TLS mit Zertifikatsprüfung |
| `redis://:password@host:6379/0` | Redis-Passwort ohne ACL-Benutzer |
| `redis+unix:///run/redis/redis.sock?db=0` | Redis-Server über Unix-Socket, weiterhin Redis-Protokoll |
| `memory://` | Isolierter In-Memory-Broker je MQ-Erzeugung |
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
programmatische Konstruktor-/Factory-Konfiguration vorzuziehen.

## § 5 Senden, empfangen und Worker-Lebenszyklus

[Programmatische Beispiele: 02-programmatic.php](../../examples/api-draft/02-programmatic.php).
Die vorgeschlagenen öffentlichen Signaturen lauten:

```php
publish(string $topic, string $type, array|object $payload,
    ?PublishOptions $options = null): PublishReceipt;
emit(object $message, ?PublishOptions $options = null): PublishReceipt;
subscribe(string|callable $topic, ?string $subscription = null,
    ?callable $handler = null, ?SubscriptionOptions $options = null): SubscriptionHandle;
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
Mapping wirft `MessageMappingException`. Registry, Attribute und explizite
Angaben ergänzen nur offene Werte; widersprüchliche feste Angaben werden
abgelehnt statt still überschrieben. Mehrfache programmatische Registrierung
desselben Sendetyps wird abgelehnt. `PublishOptions` kann eine stabile `messageId`, `expiresAt`,
`correlationId`, getrennte `metadata` und Attachments tragen. Ein Retry eines
unklar bestätigten Publishes verwendet dieselbe ID und denselben fachlichen
Inhalt. `request`/`respond` sind die optionale RPC-Erweiterung aus § 13;
`subscribe` sendet niemals automatisch einen Rückgabewert. [geändert]

`SubscriptionOptions` enthält optional `topic` und `subscription` für die
Callback-Kurzform sowie `type` als exakten Filter,
`payloadClass` als lokale Zielklasse, `startAt`, `ackMode`, `retryPolicy` und
`durability` (Default `Durability::Durable`). Memory/Unix-Tests wählen explizit
`Durability::Volatile`; damit wird keine Haltbarkeit über Prozessneustarts
versprochen. Fehlende angeforderte Haltbarkeit ist ein Capability-Fehler.
Ohne Typfilter muss der Array-Handler alle
Nachrichtentypen des Topics verarbeiten können. Nicht passende Typen werden
für diese Subscription bewusst übersprungen und bestätigt; ein separater
Handler darf nicht dieselbe Subscription mit anderem Filter übernehmen.
Filteränderungen benötigen eine neue Subscription oder explizite Migration. [geändert]

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
keinen Callback und ist kein gültiger Aufruf. [neu]

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
`#[MessageType('user.created.v1', topic: 'users', subscription: 'sdk-users')]`
auf DTOs ermöglicht `subscribe($callback)` mit einem typisierten ersten
Parameter. Alternativ reicht `#[Subscribe]` auf einer öffentlichen
Handler-Methode; `registerHandlers($object)` verwendet denselben Resolver.
Offene Werte werden am Handler oder Aufruf ergänzt. PHPDoc-Annotationen als
zweites Metadatensystem sind vorerst nicht vorgesehen; die normalen
PHPDoc-Feldtypen von `phore/schema` bleiben nutzbar. [geändert]

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
Der Resolver aus § 6.2 prüft alle vorhandenen Angaben auf Übereinstimmung. [geändert]

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
wie in § 6 beschrieben ungültig. [neu]

Der Resolver sammelt Klassenmapping/`MessageType`, ein gegebenenfalls am
Callback vorhandenes `Subscribe`-Attribut und explizite Aufruf-/Optionswerte.
Für Topic, Subscription, Typ und Zielklasse gilt: fehlend lässt sich ergänzen;
mehrere identische Angaben sind zulässig; unterschiedliche feste Angaben
sind ein Fehler. Es gibt keinen stillen Vorrang. Methodennamen, PHP-FQCNs,
Hostname oder Instanz-ID werden niemals zu Topic- oder Subscriptionnamen
umgedeutet. Für SDKs ohne Attribute kann
`registry->register($type, $class, topic: ..., subscription: ...)` dieselben
Metadaten lokal hinterlegen. [neu]

Topic und Subscription müssen nach Auflösung eindeutig vorhanden sein;
für einen DTO-Handler zusätzlich der Wire-Typ. Untypisierte/Array-Handler
benötigen explizite Topic-/Subscription-Angaben und optional den Typfilter.
Ohne Typfilter bleibt ihr bisheriger Empfang aller Typen des Topics gültig.
`payloadClass` muss weiterhin zum Callback passen. Ohne Schema-Bridge gibt
es auch in der Kurzform keine automatische DTO-Hydration. [neu]

Ein für mehrere Topics verwendeter Contract lässt `MessageType::topic`
vollständig weg. Das Topic wird für jede Registrierung explizit gewählt;
es gibt keine automatische Expansion, kein Wildcard-Abonnement und keine
Liste im `topic`-Feld. Für unabhängige Gruppen bleibt entsprechend
`MessageType::subscription` offen. Der fachliche Typ wird weiterhin aus der
Klasse übernommen: [neu]

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
logischen Topics bezeichnet unterschiedliche Bindungen. [neu]

`emit($dto)` verwendet dieselben festen Topic-/Typ-Angaben; die Subscription
spielt beim Senden keine Rolle. Ohne festes Topic sendet die Anwendung mit
`publish($topic, $type, $dto)` bzw. einem Array. Bei einem gemappten DTO müssen
explizite Topic-/Typ-Werte zu dessen festen Metadaten passen; ein offenes
Topic lässt sich frei ergänzen. `emit` ohne auflösbares Topic wirft
`MessageMappingException`. Mehrere Topics werden durch mehrere ausdrückliche
Publishes angesprochen, nicht durch einen verborgenen Broadcast. [neu]

Die Registrierung prüft alle Metadaten vor dem Anlegen/Binden von Ressourcen.
Widersprüche werfen `MessageMappingException` mit `code=MAPPING_CONFLICT`,
`field`, `sources` und den betroffenen nicht geheimen Mappingwerten; fehlende
Pflichtwerte `MAPPING_INCOMPLETE` mit `missingFields`. Ein zweiter lokaler
Handler für dieselbe Topic-/Subscription-Bindung wirft
`InvalidHandlerException` mit `code=DUPLICATE_SUBSCRIPTION`, auch wenn beide
Callbacks gleich aussehen oder verschiedene Typfilter wünschen. Andere
Prozesse derselben Gruppe sind ausdrücklich erlaubt. [neu]

`registerHandlers` prüft sämtliche ausgewählten Methoden zunächst gemeinsam
auf lokale Konflikte und bindet danach. Kein globales Dateisystem-Scanning;
eine bereits einzeln registrierte Methode darf nicht durch anschließendes
`registerHandlers` doppelt gebunden werden. Schlägt das tatsächliche Binden
am Broker teilweise fehl, werden die in diesem Aufruf neu geöffneten lokalen
Bindings geschlossen; zuvor bestehende Registrierungen bleiben erhalten.
Dabei bereits angelegte dauerhafte Brokerressourcen werden nicht automatisch
gelöscht. Die Meldung benennt die betroffenen Bindings. [neu]

Vorgesehene Contract-Tests: identisches Routing aller drei Registrierungswege,
DTO-Hydration, offene Topics, feste Topic-/Typ-/Subscription-Konflikte, fehlende
Metadaten, doppelte lokale Gruppen, gemeinsame Gruppe in zwei Prozessen,
keine Ressourcenerzeugung bei Metadatenfehlern und Cleanup bei Bindefehlern.
Beispiel 03 zeigt die erfolgreichen Varianten und erwartete Exceptions;
es bleibt ausschließlich API-Entwurf. [neu]

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
| `MessageMappingException` / `InvalidHandlerException` | `MAPPING_INCOMPLETE`, `MAPPING_CONFLICT`, `DUPLICATE_SUBSCRIPTION` oder mehrdeutige Reflection; Details in § 6.2 [geändert] |
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
verantwortet ein separater Lock-Dienst der Anwendung.

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
| Alle beteiligten Dienste informieren | `locks-service-a`, `locks-service-b`, `locks-service-c` | Jeder Dienst erhält eine Kopie und kann separat antworten |
| Alle konkreten Instanzen informieren | Je Instanz ein stabiler eigener Name, etwa `locks-instance-17` | Jede erwartete Instanz erhält ihre eigene Kopie |
| Einen Job verteilen | Alle Worker: `text-processors` | Ein verfügbarer Consumer erhält die konkrete Zustellung zur Bearbeitung |

Ein Topic kann beides gleichzeitig haben, etwa eine Worker-Subscription und
eine unabhängige Audit-Subscription. „An einen“ bedeutet deshalb nicht
weltweit exklusiv, falls daneben weitere Subscriptions existieren. Für die
Processing-Queue provisioniert man bewusst nur die ausführende Worker-Gruppe;
Audit-Consumer führen den Job nicht aus. Die ersten beiden Muster brauchen
die Fan-out-Capability: Redis-Gruppen, RabbitMQ-Queues oder SNS+SQS passen,
ein einzelnes SQS-Queue-Backend kann nicht allen Gruppen Kopien liefern.

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
**Der Subscription-Name bleibt bei allen Workern identisch.** Die Factory
erzeugt getrennte Transport-Consumer-IDs; eine Worker-ID dient im Beispiel
nur als Antwortmetadatum, nicht als neue Subscription. Der Client ruft
`request('jobs.text', 'text.process.v1', $params)->await()` auf und erhält
Payload und die Kennung des verarbeitenden Workers zurück.

Die Auswahl erfolgt brokerabhängig anhand verfügbarer Consumer, Credits,
Prefetch und Polling. „Random“ wird hier als „beliebiger verfügbarer Worker,
ohne feste Zielinstanz“ verstanden. Gleichmäßiger Zufall, Round-robin oder
garantierte Fairness sind kein portabler Vertrag; auch mehrere Jobs
hintereinander beim selben Worker sind zulässig. Wer eine bestimmte
Verteilungsstrategie benötigt, braucht einen gesonderten Scheduler.

Pro Zustellversuch wird ein Consumer ausgewählt; ein normaler Job wird
nicht an alle Worker kopiert. Bei Crash, verlorenem Ack oder Lease-Ablauf
kann derselbe Job dennoch erneut zugestellt werden. Ein pausierter alter
Worker kann nach Lease-Verlust sogar noch weiterlaufen, während ein neuer
übernimmt. „Nur ein Worker“ ist daher keine Exactly-once-/Seiteneffektgarantie:
lange Verarbeitung braucht Lease-Pflege, kritische Aktionen benötigen
Idempotenz oder ressourcenseitiges Fencing. Das Beispiel verarbeitet reinen
Text ohne externe Seiteneffekte; Ergebnis-Publish erfolgt gemäß § 13 vor
Request-Ack.

### § 15.4 Spätere Prüfungen und Quellen

Vorgesehene Contract-Tests: Broadcast an drei Subscriptions versus drei
Worker einer Gruppe, doppelte Teilnehmerantworten, fehlender/negativer
Teilnehmer, spätes Acquire nach Release, Koordinator-Crash, veraltete Lease,
falsche Teilnehmeridentität und erneute Job-Ausführung nach Lease-Verlust.
Diese Tests gehören zur späteren Implementierung, nicht zum Entwurfs-PR.

- [Redis XREADGROUP: Verteilung innerhalb von Consumer-Gruppen](https://redis.io/docs/latest/commands/xreadgroup/).
- [RabbitMQ Consumers: konkurrierende Consumer und Zustellsteuerung](https://www.rabbitmq.com/docs/consumers).
- [Redis: begrenzte Lock-Gültigkeit, Ownership und Fencing-Hinweise](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/).

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
| Ziel-Topologie | Topic/Subscription/Binding existieren, soweit der Adapter sie prüfen darf | Ohne Capability/Rechte unknown, niemals erfundener Erfolg |
| Consumer-Bereitschaft | Aktuelle Antworten der erwarteten Gruppen/Instanzen, registrierter Handler für Typ, aktive Consume-Bindung und keine blockierende Störung | Consumer-Zähler oder veraltete Redis-Gruppen allein reichen nicht |
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
Nachrichten werden nach der Lease-/Retry-Policy verzögert freigegeben oder
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

Primärquellen: [Redis PING](https://redis.io/docs/latest/commands/ping/) für
den eng begrenzten Verbindungsnachweis und [RabbitMQ Monitoring](https://www.rabbitmq.com/docs/monitoring)
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
