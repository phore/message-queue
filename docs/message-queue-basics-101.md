# Message Queue Basics 101: Vom Senden zur Verarbeitung

Ein Worker kann einen Auftrag fertig bearbeiten und trotzdem dieselbe Nachricht noch einmal erhalten.
Stell dir einen Export vor: Die ZIP-Datei ist fertig, dann stürzt der Worker ab — unmittelbar bevor er der Queue seinen Erfolg bestätigt. Die Queue weiß jetzt nur, dass ihr eine Bestätigung fehlt. Soll sie den Auftrag erneut zustellen oder riskieren, dass er verloren geht?

Dieser kleine Abstand zwischen „erledigt“ und „bestätigt“ erklärt viele Entscheidungen beim Einsatz einer Message Queue. Ein Broker vermittelt Nachrichten und kann sie aufbewahren. Er kann aber nicht allein garantieren, dass ein externer Arbeitsschritt genau einmal ausgeführt wird. Publisher-Bestätigung und Consumer-Bestätigung betreffen unterschiedliche Schritte. [RabbitMQ: Bestätigungen](https://www.rabbitmq.com/docs/confirms)

**Stand: 12. September 2026. Dieses Paket ist ein API-Entwurf. RabbitMQ ist als einziger Adapter vorgesehen und noch nicht implementiert.** Die Brokerfunktionen existieren unabhängig davon; die folgenden Zuordnungen beschreiben ihre geplante Nutzung durch PhoreMQ.

## Die Begriffe an einem Beispiel

Ein Benutzer wurde angelegt. Der Maildienst soll eine Begrüßung versenden, der Auditdienst den Vorgang protokollieren. Beide benötigen dieselbe Nachricht. Drei Mail-Worker sollen sich dagegen die Mailaufträge teilen.

| Begriff | Bedeutung im Beispiel |
|---|---|
| Producer / Publisher | Die Anwendung, die das Ereignis veröffentlicht |
| Message / Payload | Die Nachricht und ihre fachlichen Daten, etwa Benutzer-ID und E-Mail |
| Broker | Die vermittelnde Infrastruktur, in diesem Paket RabbitMQ |
| Topic | Der logische Kanal `users` in diesem Paket |
| Message type | Der fachliche Vertrag `user.created.v1`; ein Topic kann mehrere Typen tragen |
| Subscription | Ein benanntes Abonnement für die Nachrichten eines Dienstes: `mail-users` oder `audit-users` |
| Consumer / Worker | Ein laufender Prozess, der Nachrichten seiner Subscription verarbeitet |
| Subject / Routing key | Brokerabhängige Routingbegriffe; keine zusätzlichen Pflichtargumente der PhoreMQ-API |
| Envelope | Payload plus technische Angaben wie ID, Typ, Korrelation und Signatur |

Ein Topic ist hier der logische Kanal, ein Subject die Routingbezeichnung eines Nachrichtentyps. Die API verwendet dafür nur `type`. RabbitMQ bildet Topic und Subscription auf Exchange und Queue ab; der Typ wird zum Routing Key. Ein Subject ist keine separat anzulegende Ressource. Die konkrete Zuordnung und den Docker-Start zeigt der [Setup-Guide](setup.md).

## Eine Nachricht für alle — oder Arbeit für einen

Im PhoreMQ-Entwurf bestimmt die Subscription die Verteilung:

- **Unterschiedliche Subscriptions:** `mail-users` und `audit-users` erhalten jeweils eine Zustellung. Das ist Fan-out.
- **Dieselbe Subscription:** Drei Worker von `mail-users` konkurrieren um deren Zustellungen. Pro Zustellversuch übernimmt einer die Arbeit.

„Nur einer“ meint hier einen Worker, nicht einen Broker. Mehrere Brokerknoten können gemeinsam die Infrastruktur bilden und Daten replizieren. Daraus folgt nicht, dass die Anwendung denselben Job mehrfach bearbeiten soll.

Bei Lastverteilung gewinnt ein verfügbarer Worker nach den Zustellregeln von RabbitMQ. Gleichmäßiger Zufall ist nicht garantiert. Mehr Worker erlauben parallele Verarbeitung, verändern aber die Reihenfolge der Fertigstellung. Nach einem Verbindungsabbruch können sogar zwei Versuche zeitweise überlappen: Ein alter Worker arbeitet weiter, während ein anderer übernimmt. [RabbitMQ: Consumers](https://www.rabbitmq.com/docs/consumers)

## Was eine Zustellgarantie tatsächlich umfasst

Der geplante Normalfall besteht aus vier Schritten:

1. `publish()` sendet. Der Broker bestätigt seine Annahme; daraus entsteht `SendResult::receipt`.
2. Ein Worker erhält eine Zustellung, die zunächst als offen gilt.
3. Der Handler verarbeitet die Nachricht. Erst nach erfolgreicher Rückkehr erfolgt standardmäßig die Verarbeitungsbestätigung (Ack).
4. Fehlt die Bestätigung, wird der Auftrag nach Verbindungsabbruch oder gemäß Retry-Policy wieder verfügbar. Nach endgültigem Scheitern bleibt er in einer Fehlerablage. Broker verwenden dafür häufig eine Dead-Letter Queue (DLQ); die Library kann auch eine eigene Ablage nutzen.

**At least once** sagt unter den vereinbarten Aufbewahrungs- und Verfügbarkeitsbedingungen mindestens eine Zustellung an einen zuständigen Consumer zu. Solange dessen Bestätigung fehlt, sind weitere Zustellversuche möglich; Retry-Grenzen und endgültige Fehlerablage bestimmen, wann diese enden. Es ist keine unbegrenzte Erfolgsgarantie. **At most once** vermeidet Wiederholung, kann dafür Nachrichten verlieren. **Exactly once** für einen vollständigen Geschäftsprozess entsteht nicht allein durch Queue-Einstellungen.

Für den Export vom Einstieg hilft deshalb eine stabile Auftrags-ID: Der Worker erkennt einen bereits abgeschlossenen Export und verwendet dessen Ergebnis erneut. Das heißt *Idempotenz*. Ein Datenbankeintrag und ein Publish lassen sich bei Bedarf über eine Outbox koordinieren; das bleibt eine Anwendungsintegration, keine implizite Transaktion dieser Library. Auch Broker-Deduplizierung ersetzt diese Prüfung externer Seiteneffekte nicht. [RabbitMQ: Zuverlässigkeit](https://www.rabbitmq.com/docs/reliability)

## Vier Zeitgrenzen, vier verschiedene Fragen

| Grenze | Was sie beantwortet |
|---|---|
| **Retention** | Wie lange hält die Infrastruktur die Nachricht überhaupt vor? Zeit-, Größen- oder Längenlimits können sie entfernen |
| **TTL / Ablaufzeit** | Bis wann ist diese Nachricht noch gültig? Abgelaufene Nachrichten sind keine beliebig später ausführbaren Jobs |
| **Ack-Frist** | Wann beendet RabbitMQ einen Consumer-Channel wegen ausbleibender Bestätigung? Das ist keine pro Nachricht verlängerbare Lease. |
| **RPC-Wartefrist** | Wie lange wartet dieser Aufrufer auf eine Antwort? Ablauf stoppt die entfernte Arbeit nicht |

## RabbitMQ als einzige Umsetzung

Die Architektur sieht einen RabbitMQ-Adapter hinter `ConnectorInterface` vor. Eine Laufzeit-Auswahl oder Registrierung weiterer Adapter gehört nicht dazu. Die öffentliche API verwendet weiterhin Topic, Subscription und Nachrichtentyp; der Adapter kapselt das konkrete Protokoll.

Dauerhafte fachliche Subscriptions werden als Quorum Queues angelegt. Publisher Confirms bestätigen die Annahme; Consumer-Acks schließen die Verarbeitung ab. Mehrere Worker einer Queue teilen deren Arbeit. Die einzelne Docker-Instanz aus dem Setup besitzt keine Ausfallredundanz, auch wenn sie denselben Queue-Typ nutzt. [RabbitMQ Quorum Queues](https://www.rabbitmq.com/docs/quorum-queues)

## Aufbewahrung passend zur Anwendung wählen

Ein erfolgreiches Ack entfernt die Nachricht aus ihrer Subscription. Ohne Ack bleibt sie bis zur Verarbeitung, expliziten Entfernung oder einem konfigurierten Ablauf-/Größenlimit erhalten. Die Demo setzt keine automatische Nachrichten-TTL und ist kein dauerhaftes Ereignisarchiv. Die Fehlerablage benötigt ebenfalls eine bewusste Bereinigung. [RabbitMQ TTL](https://www.rabbitmq.com/docs/ttl)

Eine neue Subscription erhält nur Nachrichten ab ihrer Bindung, keine früheren Ereignisse. Ein Worker-Neustart verwendet dagegen den bestehenden Rückstand. Bei ZIP-Dateien muss zusätzlich der referenzierte Dateispeicher lang genug verfügbar bleiben.

## Was der Anwendungsentwickler konfiguriert

Verbindung und gemeinsame Vorgaben stehen in [config/message-queue.json](../config/message-queue.json). Der [Setup-Guide](setup.md) erklärt die bereits nutzbare Docker-Instanz und das PHP-Skript. Die PHP-Beispiele verwenden nach Implementierung:

```php
$mq = new PhoreMQ(...demoConnection());
```

Die Hilfsfunktion lädt ausdrücklich die gemeinsame Datei; sie gehört nur zu den Beispielen. Die Demo erlaubt mit `autoCreate` dynamische Topics und Subscriptions und wählt bewusst einen unsignierten lokalen Testmodus. Produktionskonfiguration verwendet eigene Zugangsdaten, TLS und eine passende Security-Policy. Ein Dateispeicher wird separat injiziert.

Ein Prozess kann mehrere Handler registrieren. `run(maxMessages: 100, maxSeconds: 30)` begrenzt Zustellversuche und Gesamtlaufzeit, garantiert aber keine Zahl erfolgreicher Jobs. Synchrone Handler laufen nacheinander. Mehrere Prozesse derselben Subscription ermöglichen Parallelität; `maxInFlight` begrenzt vorgeholte offene Zustellungen pro Consumer. Lange Jobs brauchen passende Ack-Fristen und laufende Heartbeat-Verarbeitung. [Worker-Beispiel](../examples/api-draft/02-programmatic.php)

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
Aufrufe kennt. Die explizit aktivierte RPC-Konfiguration erzeugt deshalb
eigene Reply-Endpunkte je MQ-Instanz im reservierten RabbitMQ-Bereich. Die
Berechtigungen dafür werden beim Deployment festgelegt. Die Request-ID verknüpft Antworten; Signaturen,
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

Nicht universell zugesagt werden Exactly-once-Seiteneffekte, globale Reihenfolge, Prioritäten, Replay oder verteilte Locks. Unbekannte Optionen, widersprüchliche Topologie und nicht routbare Nachrichten sollen früh mit aussagekräftigen Exceptions auffallen. Für den Export bedeutet das: Die Queue kann einen verlorenen Zustellversuch ersetzen. Ob eine ZIP-Datei bereits erfolgreich erstellt wurde, muss die Anwendung weiterhin zuverlässig erkennen.

## RPC nach einem Container-Neustart

Zwei Publisher senden denselben Command-Typ. Wer bekommt welche Antwort?
Jede Verbindung besitzt ein eigenes privates Antwortziel; die Request-ID trennt
die einzelnen Aufrufe darin. Der Sender richtet Queue, Binding und Consumer vor
dem Versand ein. await verarbeitet dann die eingehenden Antworten. Der Typ allein
reicht für diese Zuordnung nicht aus.

Stirbt der Publisher, verschwindet sein Rückkanal nach erkanntem Verbindungsabbruch.
Der neue Container beginnt mit einem neuen Rückkanal. Ein Timeout beweist deshalb
nicht, dass die Arbeit fehlgeschlagen ist: Sie kann bereits erledigt sein und nur
die Antwort fehlen. Für einen neuen Aufruf derselben Geschäftsoperation bleibt
die extern gespeicherte operationId gleich, während requestId und replyTo neu
sind. Der Worker kann das gespeicherte Ergebnis zurückgeben. Die Transaktion,
die Geschäftswirkung und Ergebnis zusammen schützt, gehört zur Anwendung.
[PHP-RPC-Grundprinzip](https://www.rabbitmq.com/tutorials/tutorial-six-php)

Die geplanten QueueOptions unterscheiden dauerhafte Work-/RPC-Queues und
flüchtigen Broadcast. Broadcast mit positiver Retention speichert für bereits
angelegte Empfängergruppen; es liefert keine Historie an später neue Gruppen.
Ein Ack entfernt die jeweilige Kopie früher. Detaillierter Ablauf mit Kommentaren:
[RPC-Beispiel](../examples/api-draft/05-rpc.php),
[Profile und Konflikte](../examples/api-draft/11-queue-options.php).
