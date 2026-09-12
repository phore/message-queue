# PhoreMQ im Anwendungscode

**API-Entwurf für PHP >=8.5.** Diese Dateien zeigen die geplante Nutzung; die
importierten MQ-Klassen sind noch nicht implementiert. Funktionen mit Namen wie
runUserWorker sind Worker-Entrypoints, divideFromApplication gehört in den
Anwendungs-/HTTP-Prozess. Die Dateien werden nicht gemeinsam als Skript ausgeführt.
Die Alternativen werden bewusst einzeln gewählt.

## Der Ablauf, den ein Reviewer erkennen soll

- `new PhoreMQ(...)` verbindet sofort. `demoConnection()` liefert nur Konfiguration.
- `subscribe(...)` / `respond(...)` richten den Empfangsvertrag ein; `run()` führt ihn aus.
- `publish(...)` sendet sofort. Bei reinen Events steht `reply: false` ausdrücklich dabei.
- `request(...)` sendet sofort mit Rückkanal. `await(...)` wartet auf genau diesen Auftrag.
- Der Callback erhält die Payload; `MessageContext` bzw. `RequestContext` gehört zu dieser Zustellung.
- `finally { $mq->close(); }` schließt eine selbst erzeugte Connection. Injizierte Connections bleiben beim Aufrufer.

Der Worker muss seine Subscription vor dem ersten Publisher eingerichtet haben.
Eine Broker-Bestätigung beweist keine Verarbeitung. Bei mehreren Prozessen teilen
sich Worker einer dauerhaften Subscription die Arbeit; unterschiedliche Gruppen
erhalten eigene Kopien. Ein Callback läuft erst in run, ein RPC-Ergebnis wird in
await gelesen. Auf derselben Connection erst zu warten und danach den benötigten
Responder zu starten funktioniert nicht.

## Die Beispiele einzeln

| Datei | Anwendungseinstieg / Reihenfolge | Nutzerfrage und Änderung nach Review |
|---|---|---|
| [01-connect.php](01-connect.php) | connectDemo, connectConfigured oder connectDirectly auswählen | „Ist das schon verbunden?“ Ja; der Aufrufer besitzt und schließt die zurückgegebene Connection. |
| [02-programmatic.php](02-programmatic.php) | runUserWorker zuerst, dann publishUserCreated | „Registrieren oder verarbeiten?“ Sender und Worker sind getrennt; Batch-Limits stehen in einer separaten Variante. |
| [03-attributes.php](03-attributes.php) | runAttributeWorker ODER runCallbackWorker, dann publishUserCreated | „Woher kommt das Routing?“ Aus dem DTO/Registry; es wird nicht doppelt registriert. cancel entfernt keinen Broker-Vertrag. |
| [04-files-and-local.php](04-files-and-local.php) | transferArchive mit injiziertem Speicher | „Gehen ZIP-Bytes durch RabbitMQ?“ publish speichert die Datei und sendet die Referenz; copyTo verifiziert beim Empfang. Ein Prozess dient hier nur der Vorführung. |
| [05-rpc.php](05-rpc.php) | runServer zuerst, dann eine divide*-Funktion | „Wann wird gesendet, was wirft?“ Kurzer Hauptpfad; Notices, typisierte Fehler und zweites Warten sind getrennte Anwendungsfälle. |
| [06-metadata-middleware.php](06-metadata-middleware.php) | processOrderWithDiagnostics | „Ist der Fehler verarbeitet oder verschluckt?“ Middleware reicht return/Exception weiter; der konkrete Reject-Pfad und die Diagnose werden ausgeführt. |
| [07-broadcast-locking.php](07-broadcast-locking.php) | je Teilnehmer runParticipant; ein withAllLocks-Koordinator | „Antworten wirklich alle?“ Eigene Subscription je Teilnehmer und expliziter Snapshot; lokale Leases/Fencing sind Anwendungsabhängigkeiten. |
| [08-processing-workers.php](08-processing-workers.php) | mehrere runWorker mit gleicher Gruppe; submitJobs | „Wer bekommt den Job?“ Ein verfügbarer Worker; Fehler je Auftrag abfangen, übrige bereits gesendete Aufträge weiter auswerten. |
| [09-system-check.php](09-system-check.php) | runExportWorker; checkAtLogin im Backend | „Prüfe ich Broker oder Funktion?“ Ein gezielter Login-Check; Gesamtcheck und Statusbeobachtung sind eigene Funktionen. |
| [10-callback-errors.php](10-callback-errors.php) | processFailureScenarios auf frischem Namespace | „Retry oder Ende?“ Temporär, permanent und unerwartet sind explizit; unbekannter Modus wird nicht versehentlich als Erfolg bestätigt. |
| [11-queue-options.php](11-queue-options.php) | typisierten ODER programmatischen Worker wählen | „An welchem Objekt ändere ich die Queue?“ QueueOptions am Subscriber/DTO; Live-Broadcast und dauerhafte Retention haben getrennte Einstiege und Namen. |

## Welches Objekt ändere ich?

| Absicht | Stelle |
|---|---|
| Verbindung, Security, Middleware oder globale Defaults | ConnectionOptions beim Erzeugen der PhoreMQ-Instanz |
| Wire-Typ und DTO-Struktur | MessageType-Attribut / MessageRegistry, vor Registrierung bzw. Konstruktor |
| Eigene Queue, Retention und Retry-Abstand | SubscriptionOptions.queue / Queue-Attribut |
| Parameter, Metadaten, Attachments, Antwortfrist senden | Payload und PublishOptions bei publish/request |
| Antwort hydrieren, Remote-Fehler zuordnen, lokal warten | AwaitOptions oder direkte Parameter bei SendResult.await |
| Eine Zustellung quittieren oder Begleitmeldung senden | MessageContext / RequestContext im Handler |
| Einen Handler lokal entfernen | Zurückgegebenes SubscriptionHandle.cancel |
| Dienstproblem melden | Das injizierte HealthState-Objekt |

`PublishOptions(replyTimeoutSeconds: 15)` setzt die Antwortfrist **ab Versand**.
`await(timeoutSeconds: 5)` wartet lokal höchstens fünf Sekunden und nie über diese
Antwortfrist hinaus. Ein zweites await desselben SendResult sendet nichts erneut.
Vier Retry-Versuche mit zehn Sekunden Abstand passen nicht garantiert in eine
15-Sekunden-Frist. Timeout bedeutet unbekannter Ausgang, keine Rücknahme.

Die generische Form publish(...)->await bleibt möglich, wenn vorher ein Rückkanal
aktiviert wurde. Die Demo-Konfiguration aktiviert RPC global; ohne diesen Kontext
ist allein publish(...)->await nicht selbsterklärend. Die Hauptbeispiele benutzen
deshalb request für RPC und reply:false für reine Events. Beide Sendemethoden
verwenden PublishOptions; es gibt keine zusätzliche RequestOptions-Klasse.

## Fehler und Grenzen beim Lesen

Ein erfolgreich zurückgekehrter subscribe-Callback bestätigt die Verarbeitung.
Sein Rückgabewert wird nicht zum Reply; dafür verwendet man respond. Retryable-
und Reject-Exceptions verarbeitet die Runtime nach den QueueOptions; sie müssen
nicht aus run herausgeworfen werden. Transport-/Fehlerablageprobleme bleiben
sichtbare Infrastruktur-Exceptions. Ein RemoteCommandException entsteht beim
await; eine RequestTimeoutException kann auch nach erfolgreicher Geschäftsaktion
auftreten. Sichere Wiederholung schreibender Aufträge braucht persistente Idempotenz.

Die Beispiele 02–03 benötigen bei DTOs die Schema-Bridge. Der manuelle Array-Pfad
hat nur dort automatische Strukturprüfung, wo ein Contract registriert wurde.
Produktionsabhängigkeiten wie Dateispeicher und Lease-Verwaltung sind ausdrücklich
injiziert und werden nicht durch wirkungslose Attrappen ersetzt.

[Verbindung und Docker-Setup](../../docs/setup.md) ·
[Vollständiger API-Vertrag](../../docs/proposals/2026-09-12-message-queue-api.md)
