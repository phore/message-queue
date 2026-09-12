# Message Queue Basics 101: Vom Senden zur Verarbeitung

Ein Worker kann einen Auftrag fertig bearbeiten und trotzdem dieselbe Nachricht noch einmal erhalten.
Stell dir einen Export vor: Die ZIP-Datei ist fertig, dann stürzt der Worker ab — unmittelbar bevor er der Queue seinen Erfolg bestätigt. Die Queue weiß jetzt nur, dass ihr eine Bestätigung fehlt. Soll sie den Auftrag erneut zustellen oder riskieren, dass er verloren geht?

Dieser kleine Abstand zwischen „erledigt“ und „bestätigt“ erklärt viele Entscheidungen beim Einsatz einer Message Queue. Ein Broker vermittelt Nachrichten und kann sie aufbewahren. Er kann aber nicht allein garantieren, dass ein externer Arbeitsschritt genau einmal ausgeführt wird. Publisher-Bestätigung und Consumer-Bestätigung betreffen unterschiedliche Schritte. [RabbitMQ: Bestätigungen](https://www.rabbitmq.com/docs/confirms)

**Stand: 12. September 2026. Dieses Paket ist ein API-Entwurf. Noch keiner der hier genannten Adapter ist implementiert.** Die Brokerfunktionen existieren unabhängig davon; die folgenden Zuordnungen beschreiben ihre geplante Nutzung durch PhoreMQ.

## Die Begriffe an einem Beispiel

Ein Benutzer wurde angelegt. Der Maildienst soll eine Begrüßung versenden, der Auditdienst den Vorgang protokollieren. Beide benötigen dieselbe Nachricht. Drei Mail-Worker sollen sich dagegen die Mailaufträge teilen.

| Begriff | Bedeutung im Beispiel |
|---|---|
| Producer / Publisher | Die Anwendung, die das Ereignis veröffentlicht |
| Message / Payload | Die Nachricht und ihre fachlichen Daten, etwa Benutzer-ID und E-Mail |
| Broker | Die vermittelnde Infrastruktur, beispielsweise Redis oder RabbitMQ |
| Topic | Der logische Kanal `users` in diesem Paket |
| Message type | Der fachliche Vertrag `user.created.v1`; ein Topic kann mehrere Typen tragen |
| Subscription | Ein benanntes Abonnement für die Nachrichten eines Dienstes: `mail-users` oder `audit-users` |
| Consumer / Worker | Ein laufender Prozess, der Nachrichten seiner Subscription verarbeitet |
| Subject / Routing key | Brokerabhängige Routingbegriffe; keine zusätzlichen Pflichtargumente der PhoreMQ-API |
| Envelope | Payload plus technische Angaben wie ID, Typ, Korrelation und Signatur |

Ein NATS-*Subject* ist die Routingadresse einer Nachricht. Ein JetStream-Stream kann Nachrichten mehrerer Subjects speichern; ein Consumer bestimmt, welche davon er liest. Bei RabbitMQ routet dagegen eine Exchange anhand von Bindings und gegebenenfalls Routing Keys in Queues. Diese Begriffe lassen sich nicht überall eins zu eins auf „Topic“ abbilden. Der Adapter übernimmt die Zuordnung. [NATS: Streams](https://docs.nats.io/learn/jetstream/your-first-stream), [RabbitMQ: Exchanges](https://www.rabbitmq.com/docs/exchanges)

## Eine Nachricht für alle — oder Arbeit für einen

Im PhoreMQ-Entwurf bestimmt die Subscription die Verteilung:

- **Unterschiedliche Subscriptions:** `mail-users` und `audit-users` erhalten jeweils eine Zustellung. Das ist Fan-out.
- **Dieselbe Subscription:** Drei Worker von `mail-users` konkurrieren um deren Zustellungen. Pro Zustellversuch übernimmt einer die Arbeit.

„Nur einer“ meint hier einen Worker, nicht einen Broker. Mehrere Brokerknoten können gemeinsam die Infrastruktur bilden und Daten replizieren. Daraus folgt nicht, dass die Anwendung denselben Job mehrfach bearbeiten soll.

Bei Lastverteilung gewinnt ein verfügbarer Worker nach den Regeln des jeweiligen Brokers. Gleichmäßiger Zufall ist nicht garantiert. Mehr Worker erlauben parallele Verarbeitung, verändern aber die Reihenfolge der Fertigstellung. Nach einem Verbindungs- oder Lease-Verlust können sogar zwei Versuche zeitweise überlappen: Ein alter Worker arbeitet weiter, während ein anderer übernimmt. [RabbitMQ: Consumers](https://www.rabbitmq.com/docs/consumers), [Redis: Consumer Groups](https://redis.io/docs/latest/commands/xreadgroup/)

## Was eine Zustellgarantie tatsächlich umfasst

Der geplante Normalfall besteht aus vier Schritten:

1. `publish()` sendet. Der Broker bestätigt seine Annahme; daraus entsteht `SendResult::receipt`.
2. Ein Worker erhält eine Zustellung, die zunächst als offen gilt.
3. Der Handler verarbeitet die Nachricht. Erst nach erfolgreicher Rückkehr erfolgt standardmäßig die Verarbeitungsbestätigung (Ack).
4. Fehlt die Bestätigung, wird der Auftrag nach den Regeln für Lease und Retry wieder verfügbar. Nach endgültigem Scheitern bleibt er in einer Fehlerablage. Broker verwenden dafür häufig eine Dead-Letter Queue (DLQ); die Library kann auch eine eigene Ablage nutzen.

**At least once** sagt unter den vereinbarten Aufbewahrungs- und Verfügbarkeitsbedingungen mindestens eine Zustellung an einen zuständigen Consumer zu. Solange dessen Bestätigung fehlt, sind weitere Zustellversuche möglich; Retry-Grenzen und endgültige Fehlerablage bestimmen, wann diese enden. Es ist keine unbegrenzte Erfolgsgarantie. **At most once** vermeidet Wiederholung, kann dafür Nachrichten verlieren. **Exactly once** für einen vollständigen Geschäftsprozess entsteht nicht allein durch Queue-Einstellungen.

Für den Export vom Einstieg hilft deshalb eine stabile Auftrags-ID: Der Worker erkennt einen bereits abgeschlossenen Export und verwendet dessen Ergebnis erneut. Das heißt *Idempotenz*. Ein Datenbankeintrag und ein Publish lassen sich bei Bedarf über eine Outbox koordinieren; das bleibt eine Anwendungsintegration, keine implizite Transaktion dieser Library. Auch Broker-Deduplizierung ersetzt diese Prüfung externer Seiteneffekte nicht. [RabbitMQ: Zuverlässigkeit](https://www.rabbitmq.com/docs/reliability)

## Vier Zeitgrenzen, vier verschiedene Fragen

| Grenze | Was sie beantwortet |
|---|---|
| **Retention** | Wie lange hält die Infrastruktur die Nachricht überhaupt vor? Zeit-, Größen- oder Längenlimits können sie entfernen |
| **TTL / Ablaufzeit** | Bis wann ist diese Nachricht noch gültig? Abgelaufene Nachrichten sind keine beliebig später ausführbaren Jobs |
| **Lease / Visibility / Ack-Frist** | Wie lange darf ein Worker die offene Zustellung bearbeiten, bevor sie erneut verfügbar werden kann? |
| **RPC-Wartefrist** | Wie lange wartet dieser Aufrufer auf eine Antwort? Ablauf stoppt die entfernte Arbeit nicht |

## Welche Adapter vorgesehen sind

Alle Statusangaben beziehen sich auf dieses Paket, nicht auf die Reife des jeweiligen Brokers.

| Adapter / Status | Fan-out und konkurrierende Worker | Bestätigung und Wiederholung | Aufbewahrung und wichtigste Grenze |
|---|---|---|---|
| **Redis Streams — erster produktiver Adapter geplant** | Eigene Gruppe je Subscription; Worker teilen eine Gruppe | `XACK` entfernt den Eintrag aus der Pending-Liste der Gruppe; Claim übernimmt verwaiste Zustellungen | Stream bleibt bis Trimming/Löschung; Ack allein löscht den Streameintrag nicht. Persistenz und Eviction müssen passend konfiguriert sein |
| **Redis Pub/Sub — möglicher späterer Modus** | Aktive Subscriber erhalten Nachrichten | Keine dauerhafte Ack-/Recovery-Semantik | Keine Historie für offline gegangene Subscriber; kein Ersatz für Streams |
| **SQS — geplanter Arbeitsqueue-Adapter** | Worker teilen eine Queue; eine Queue allein liefert keinen unabhängigen Fan-out | Visibility Timeout verbirgt die Zustellung vorübergehend, `DeleteMessage` bestätigt die Verarbeitung | Standardmäßig 4 Tage, konfigurierbar bis 14 Tage; Standard Queues erlauben Duplikate und bieten keine strenge Reihenfolge |
| **SNS + SQS — geplanter Fan-out-Adapter** | SNS verteilt an eine SQS-Queue je Subscription; deren Worker teilen Arbeit | Annahme durch SNS und Zustellung nach SQS sind eigene Schritte; Retry-/DLQ-Konfiguration nötig | Nach Übergabe gilt die Retention der jeweiligen SQS-Queue; nicht als universelles Event-Archiv behandeln |
| **Azure Service Bus — geplant** | Queues für Arbeitsverteilung, Topics mit Subscriptions für Fan-out | Peek-Lock, `Complete`, `Abandon`, Lock-Erneuerung und Dead Letter | TTL und Zustellgrenzen konfigurieren; Receive-and-Delete passt nicht zum geplanten Auto-Ack-nach-Erfolg |
| **RabbitMQ — geplant** | Exchange/Bindings routen in Queues; Consumer einer Queue teilen Arbeit | Publisher Confirms und Consumer-Acks; Requeue/Dead Letter | Queue-/Message-TTL und Längenlimits konfigurieren; dauerhafte Queue, persistente Nachrichten und geeigneter Queue-Typ gehören zum Haltbarkeitskonzept |
| **NATS JetStream — später geplant** | Eigenständige Consumer für unabhängige Sichten; gemeinsam genutzter Pull-Consumer für Worker | Stream-Publish-Ack und explizites Consumer-Ack, Redelivery nach Ack-Frist | Limits-, Interest- und WorkQueue-Retention unterscheiden sich. WorkQueue passt nicht zu beliebig überlappenden unabhängigen Consumern |
| **In-Memory — für Tests geplant** | Lokale Gruppen am selben ausdrücklich geteilten Brokerobjekt | Vom Testadapter nachgebildet | Prozessende verliert Zustand; keine Kommunikation zwischen unabhängigen Prozessen |
| **file — lokaler Entwicklungsadapter geplant** | Prozesse desselben OS-Benutzers teilen einen SQLite-Queue-Root | Transaktionale Claims, Leases und Fehlerablage sind Teil des Entwurfs | Lokale Dateien überleben Neustarts; Retention/Bereinigung nötig. Kein NFS-/Multi-Host-Adapter |
| **Unix-Dev-Broker — Anschlussphase geplant** | Separater lokaler Prozess verwaltet Gruppen | Eigenes lokales MQ-Protokoll erforderlich | Im Entwurf volatil; Broker-Neustart verliert Zustand. Ein Unix-Socket allein ist keine Queue |

Belege zu den Brokerzeilen: [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/), [Redis Pub/Sub](https://redis.io/docs/latest/develop/pubsub/), [SQS Retention](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-queue-parameters.html), [SQS Standard Queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues.html), [SNS an SQS](https://docs.aws.amazon.com/sns/latest/dg/sns-sqs-as-subscriber.html), [Azure Settlement](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-transfers-locks-settlement), [RabbitMQ Confirms](https://www.rabbitmq.com/docs/confirms), [JetStream Retention](https://docs.nats.io/learn/jetstream/retention-policies). Die drei lokalen Adapterzeilen sind eigene Designentscheidungen; Details im [API-Proposal](proposals/2026-09-12-message-queue-api.md).

FIFO-Funktionen, etwa bei SQS, sowie Sessions oder geordnete Consumer können bestimmte Reihenfolgen absichern. Sie sind keine universelle Zusage dieser Abstraktion und müssen später als konkrete Adapter-Capability beschrieben werden. Core NATS ist außerdem von JetStream zu unterscheiden: Sein gewöhnlicher Pub/Sub-Betrieb ist kein persistenter JetStream-Consumer. [SQS FIFO](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html), [NATS: JetStream](https://docs.nats.io/learn/jetstream)

## Aufbewahrung passend zur Anwendung wählen

Es gibt daher keine gemeinsame Antwort „alle Nachrichten bleiben sieben Tage“. Bei Redis kann Trimming die Historie kürzen, bei SQS gilt die Queue-Retention, bei JetStream zusätzlich die gewählte Retention-Policy. Azure kann abgelaufene Nachrichten je Einstellung entfernen oder dead-lettern. Auch Dead-Letter-Speicher brauchen eine eigene Aufbewahrungs- und Bereinigungsregel. [Redis XTRIM](https://redis.io/docs/latest/commands/xtrim/), [Azure Ablaufzeiten](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-expiration)

Die Aufbewahrung muss zu erwarteten Ausfällen und Wiederholungen passen. Bei großen ZIP-Dateien gilt das zusätzlich für die ausgelagerte Datei: Eine erhaltene Nachricht mit bereits gelöschtem Attachment ist nicht mehr vollständig verarbeitbar.

## Was der Anwendungsentwickler konfiguriert

Für die **geplanten lokalen Beispiele** reicht:

```php
$mq = new PhoreMQ('file:///tmp/phore-mq-demo');
```

Das lokale Profil bündelt Queue, Fehlerablage, Attachment-Store, einen persistenten lokalen Signaturschlüssel und eindeutige RPC-Rückkanäle. Voraussetzungen und die explizite Redis-/Factory-Konfiguration stehen einmalig in [01-connect.php](../examples/api-draft/01-connect.php). Es ist kein Produktions-Sicherheitsprofil für mehrere Dienste oder Benutzer.

Danach entscheidet die Anwendung vor allem über drei Dinge: **Routing** (wer braucht welche Nachrichten?), **Lebensdauer** (wie lange darf Arbeit offen bleiben?) und **Fehlerbehandlung** (wann erneut versuchen, wann endgültig ablegen?). Topics, Bindings und Berechtigungen werden bei Netzwerkbrokern in Produktion vorab provisioniert; die Library legt sie nur mit ausdrücklich erlaubtem `autoCreate` an.

Ein Prozess kann mehrere Handler registrieren. `run(maxMessages: 100, maxSeconds: 30)` verarbeitet ihre Zustellungen, bis das erste Limit erreicht ist. Es bedeutet weder 100 parallele Jobs noch garantiert 100 erfolgreiche Jobs. Synchrone Handler laufen in einem Worker nacheinander; für Parallelität startet die Anwendung mehrere Worker derselben Subscription. Lange Jobs brauchen passende Leases oder deren Verlängerung. Details und Rückkehrbedingungen stehen in [02-programmatic.php](../examples/api-draft/02-programmatic.php).

## Wie eine Antwort zum richtigen Aufrufer zurückkommt

Zwei Frontend-Prozesse bestellen gleichzeitig einen Export. Beide senden
`export.create.v1`. Der Nachrichtentyp allein kann ihre Antworten deshalb nicht
zuordnen. Im geplanten RPC-Protokoll werden zwei andere Angaben kombiniert:
eine eindeutige **Request-ID pro Aufruf** und ein **Reply-Ziel pro Client-Instanz**.

| Aufrufer | Gesendeter Typ | Request-ID | Eigener Rückkanal |
|---|---|---|---|
| Client A | `export.create.v1` | `request-A-17` | `replies.client-A` |
| Client B | `export.create.v1` | `request-B-42` | `replies.client-B` |

Die Namen und IDs in der Tabelle sind verkürzte Beispiele. Vor dem Senden
legt die Library den Rückkanal an bzw. bestätigt seine Bindung und registriert
die Request-ID lokal. Der Worker übernimmt das erlaubte Reply-Ziel aus dem
Request und setzt dessen Request-ID in seine Antwort. Client A empfängt die Antwort über seinen Rückkanal und ordnet sie anhand der Request-ID dem richtigen offenen Aufruf zu. Antwortet der Worker sehr schnell, ist die Zuordnung
bereits vorbereitet — sie entsteht nicht erst beim späteren `await()`.

**Zwei unabhängige Clients dürfen nicht dieselbe konkurrierende Reply-Subscription
benutzen.** Sonst könnte A die Antwort für B abholen. Ein geteilter Rückkanal
benötigt stattdessen einen bewusst eingerichteten zentralen Verteiler, der alle
Aufrufe kennt. Die Standardeinstellung des lokalen file-Profils erzeugt deshalb
eigene Reply-Endpunkte je MQ-Instanz. Bei Netzwerkbrokern werden diese und ihre
Berechtigungen konfiguriert. Die Request-ID verknüpft Antworten; Signaturen,
zugelassene Reply-Ziele und verifizierte Identitäten schützen zusätzlich vor
fremden Antworten. Eine erratene oder kopierte ID ist keine Berechtigung.

Doppelte Antworten werden anhand ihrer Zuordnung erkannt; nur die erste
passende verifizierte finale Antwort beendet den Aufruf. Fremde oder zu spät
eingetroffene Antworten dürfen keinen anderen Aufruf erfüllen. Ein nachträgliches
`await()` sendet den Auftrag nicht erneut. Eine fachliche `correlationId`, etwa
für einen gesamten Bestellvorgang, kann dagegen mehrere Requests verbinden und
ersetzt deshalb nicht die eindeutige Request-ID. Das sind Regeln des
[geplanten RPC-Vertrags](proposals/2026-09-12-message-queue-api.md),
keine automatische Eigenschaft eines Nachrichtentypnamens.

## Wie derselbe Auftrag ohne doppelte Wirkung wiederholt werden kann

Eine gemeinsame Worker-Gruppe verteilt normalerweise jeden Zustellversuch
an einen Worker. Ein Ack sagt der Queue anschließend, dass dieser Versuch
abgeschlossen ist. Beides beantwortet noch nicht, ob eine Wiederholung einen
zweiten Export oder eine zweite Abbuchung auslösen würde.

Für eine dauerhaft gespeicherte Geschäftsoperation braucht die Anwendung eine
stabile **Idempotenz-ID pro fachlichem Auftrag**. Wiederholt der Aufrufer denselben
Auftrag nach einem Timeout, muss er dieselbe fachliche ID verwenden. Eine neu
erzeugte Message-ID bei jedem Publish erkennt solche Wiederholungen nicht.
Die Anwendung kann diese ID im Command als `operationId` mitgeben; sie ist von
der Request-ID des einzelnen RPC-Aufrufs getrennt. Die ID gilt innerhalb eines
klaren Mandanten-/Command-Bereichs; dieselbe ID mit anderen Parametern ist ein
Konflikt und darf nicht still das frühere Ergebnis liefern.

Ein mögliches Verfahren innerhalb einer Datenbank sieht so aus:

1. Ein eindeutiger Datenbankindex verhindert, dass zwei Worker denselben
   Auftrag unabhängig als neu anlegen.
2. Fachliche Änderung und gespeichertes Ergebnis werden in derselben
   Transaktion festgeschrieben. Ein bloßes „gesehen“-Flag vor der Arbeit wäre
   zu früh: Nach einem Crash könnte es einen unerledigten Auftrag sperren.
3. Bei einer Wiederholung wird das fertige Ergebnis geladen und zurückgegeben,
   ohne die Geschäftsoperation erneut auszuführen. Das gespeicherte fachliche
   Ergebnis wird dabei in eine neue Antwort mit der aktuellen Request-ID und
   dem aktuellen erlaubten Reply-Ziel verpackt. Auch ein bereits laufender
   Auftrag braucht eine definierte Warte-/Konflikt- und Recovery-Regel.
4. Die Queue erhält ihr Ack erst nach der erforderlichen Verarbeitung und,
   bei RPC, nach bestätigtem Antwort-Publish. Ein verlorenes Ack kann trotzdem
   eine erneute Zustellung auslösen; diese findet nun das gespeicherte Ergebnis.

Das schützt nur die Wirkungen innerhalb der gewählten Transaktionsgrenze.
Eine externe Zahlung oder E-Mail ist nicht automatisch Teil der lokalen
Datenbanktransaktion. Dafür braucht es eine passende Schnittstelle des Zielsystems,
beispielsweise einen Idempotenzschlüssel beim Zahlungsanbieter. Bei Leases können
zusätzlich **Fencing-Tokens** nötig sein: Die schreibende Ressource lehnt einen
veralteten Worker anhand einer monotonen Berechtigungsnummer ab. Ein lokales
„schon gesehen“-Set im PHP-Prozess überlebt keinen Neustart und koordiniert keine
anderen Prozesse.

Der Entwurf verspricht daher wiederholbare Zustellung und Erweiterungspunkte
für Idempotenz beziehungsweise Inbox/Outbox, aber keine universelle Exactly-once-
Verarbeitung. Für den ZIP-Export kann die Anwendung etwa den fertigen Export
unter seiner Auftrags-ID wiederfinden; wie sie Datei und Ergebnisstatus
crashsicher zusammenführt, muss sie ausdrücklich festlegen.
[Entwurfsgrenzen und Idempotenz](proposals/2026-09-12-message-queue-api.md),
[RabbitMQ: Redelivery und Idempotenz](https://www.rabbitmq.com/docs/reliability)

## Wie PhoreMQ darauf aufbaut

`publish($dto)` übernimmt Topic und Typ aus lokal hinterlegten Metadaten; alternativ werden Topic, Typ und Payload ausdrücklich angegeben. `subscribe($callback)` kann das Mapping des ersten DTO-Parameters verwenden. Werden Topic, Typ oder Subscription sowohl ausdrücklich angegeben als auch in lokalen DTO-Metadaten festgelegt, müssen die jeweiligen Angaben übereinstimmen. Die PHP-Klasse des Senders muss nicht dieselbe wie beim Empfänger sein: Der fachliche Typname und die strukturelle Kompatibilität sind entscheidend.

RPC (Remote Procedure Call) ist ein entfernter Funktionsaufruf mit Antwort und verwendet hier denselben Transport. `publish($command)->await(timeoutSeconds: 5)` wartet auf ein Responder-Ergebnis. Ohne rechtzeitige Antwort wirft es `RequestTimeoutException`; eine zur Übertragung freigegebene fachliche Fehlerantwort wird dagegen als `RemoteCommandException` oder ausdrücklich zugeordnete eigene Exception geworfen. `await()` sendet nichts erneut. [RPC-Beispiel](../examples/api-draft/05-rpc.php)

Bei Callback-Exceptions sieht der Entwurf begrenzte Wiederholungen mit wachsender Wartezeit (Backoff) und danach die Speicherung in einer Fehlerablage vor. Eine ausdrücklich endgültige Ablehnung wird nicht erneut versucht. Scheitert die Speicherung in der Fehlerablage, wird die ursprüngliche Nachricht nicht bestätigt. [Fehlerbeispiel](../examples/api-draft/10-callback-errors.php)

`check()` unterscheidet Verbindung und nachrichtenspezifische Bereitschaft. Ein erreichbarer Broker beweist noch keinen bereiten Handler; ein fehlendes Ping-Reply beweist nicht die Abwesenheit eines Listeners. Statusmeldungen haben deshalb Quellen und Ablaufzeiten. [Systemcheck](../examples/api-draft/09-system-check.php)

Nicht universell zugesagt werden Exactly-once-Seiteneffekte, globale Reihenfolge, Prioritäten, Replay oder verteilte Locks. Fehlende Adapterfähigkeiten sollen früh als `UnsupportedCapabilityException` auffallen. Für den Export bedeutet das: Die Queue kann einen verlorenen Zustellversuch ersetzen. Ob eine ZIP-Datei bereits erfolgreich erstellt wurde, muss die Anwendung weiterhin zuverlässig erkennen.
