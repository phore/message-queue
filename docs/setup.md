# RabbitMQ starten und PhoreMQ konfigurieren

PhoreMQ wird zunächst ausschließlich für RabbitMQ umgesetzt. Der Adapter bleibt
hinter einem Interface; eine Brokerauswahl oder Treiberregistrierung gibt es nicht.
Die Anwendungsbegriffe sind **Namespace, Topic, Subscription und Nachrichtentyp**.

**Bereits verwendbar:** Docker Compose und das PHP-Setup in diesem Guide.
**Noch Entwurf:** sämtliche `PhoreMQ`-Klassen und PHP-Beispiele. Der Container
installiert keine PHP-Library; `composer install` macht die Entwurfs-API nicht ausführbar.

## 1. Eine temporäre Instanz starten

Voraussetzung: Docker mit Compose v2 und PHP >= 8.5 CLI (für HTTP-Zugriffe mit `allow_url_fopen=1`). Alle Befehle laufen aus dem
Repository-Verzeichnis:

```bash
docker compose -f deployment/rabbitmq/compose.yaml up -d --wait
php deployment/rabbitmq/setup.php
```

Der erste Befehl startet RabbitMQ mit Management-Plugin. Der zweite legt die
fachlichen Topics, Subscriptions und zugehörigen Fehlerqueues aus
[config/message-queue.json](../config/message-queue.json) an. Er kann nach
unveränderter Konfiguration erneut ausgeführt werden. Falls die Management-API
beim ersten Aufruf noch nicht bereit ist, den zweiten Befehl erneut ausführen.

| Zugang | Wert |
|---|---|
| AMQP-Verbindung vom Host | `amqp://demo:demo@127.0.0.1:5672/demo` |
| Management-Oberfläche | `http://127.0.0.1:15672` |
| Benutzer / Passwort | `demo` / `demo` |
| Namespace | `demo` |

Die Ports sind ausschließlich an Loopback gebunden. Die Zugangsdaten und der
explizit unsignierte Nachrichtenmodus sind Entwicklungswerte. Das Image
`rabbitmq:4.3-management` folgt der Patch-Serie; reproduzierbare produktive
Deployments pinnen einen geprüften Image-Digest. Docker-Dokumentation:
[offizielles RabbitMQ-Image](https://hub.docker.com/_/rabbitmq).

Der Broker verwendet ein benanntes Volume. Ein Neustart erhält den Zustand:

```bash
docker compose -f deployment/rabbitmq/compose.yaml restart
```

Wenn die Demo nicht mehr benötigt wird, entfernt dieser Befehl Container und
**alle Nachrichten und Einstellungen ihres Volumes**:

```bash
docker compose -f deployment/rabbitmq/compose.yaml down -v
```

Ein anschließendes `up -d --wait` und `setup.php` ergeben einen frischen Demo-Broker.
Die Demo nutzt einen einzelnen Knoten, besitzt also keine Ausfallredundanz.
Für produktive Quorum Queues sind üblicherweise drei Knoten in getrennten
Ausfallbereichen vorgesehen. [RabbitMQ Quorum Queues](https://www.rabbitmq.com/docs/quorum-queues)

## 2. Eine gemeinsame Konfiguration

Die JSON-Datei enthält Verbindung, Laufzeitoptionen und fachliche Topologie.
Die Konfiguration spricht absichtlich nicht von Exchanges oder Bindings:

```json
{
  "connection": "amqp://demo:demo@127.0.0.1:5672/demo",
  "options": {
    "autoCreate": true,
    "managementUrl": "http://127.0.0.1:15672",
    "maxInFlight": 1,
    "security": {"mode": "unsigned"},
    "rpc": {"enabled": true, "replyNamespace": "_phore.rpc"}
  },
  "topics": ["users"],
  "subscriptions": [
    {"topic": "users", "name": "audit-users", "type": "user.created.v1"},
    {"topic": "users", "name": "billing-users", "type": "user.created.v1"}
  ]
}
```

Dies ist ein verkürzter Ausschnitt; die vollständige Datei enthält auch `telemetry`.
Der Namespace steht im DSN-Pfad. Sonderzeichen in Benutzer, Passwort oder
Namespace werden percent-kodiert; der Namespace `/` lautet `/%2F` im DSN.
Ein Client in einem anderen Container desselben Compose-Netzes verwendet
`rabbitmq:5672` als Host und Port; `127.0.0.1` bezeichnet dort seinen eigenen Container.

| Einstellung | Bedeutung |
|---|---|
| `connection` | Genau eine Verbindung; `amqp` lokal, `amqps` mit geprüften TLS-Zertifikaten im Betrieb |
| `topics` | Logische Kanäle, die das Setup vorher einrichtet |
| `subscriptions[].name` | Dauerhafter Gruppenname; mehrere Prozesse mit diesem Namen teilen Arbeit |
| `subscriptions[].type` | Ein exakter Nachrichtentyp; `null` empfängt alle Typen des Topics |
| `autoCreate` | Erlaubt die dynamische Anlage beim Registrieren von Listenern; Publisher legen nichts an; Standard false, Demo true |
| `managementUrl` | Expliziter Verwaltungsendpunkt für die vollständige Topologieprüfung; im Betrieb HTTPS und begrenzte Rechte |
| `maxInFlight` | Maximale Anzahl unbestätigter Zustellungen pro Consumer; kein zusätzlicher PHP-Thread |
| `security.mode` | In dieser isolierten Demo explizit `unsigned`; produktiv eine passende HMAC-Policy konfigurieren |
| `rpc.enabled` | Bereitet pro Client einen eigenen Rückkanal vor, damit späteres `await()` möglich ist |
| `rpc.replyNamespace` | Reservierter Bereich für interne Antwortziele; keine gemeinsame konkurrierende Reply-Queue |

`setup.php` verarbeitet ausschließlich `connection`, `topics` und `subscriptions`;
`options` sind Vorgaben für die geplante PHP-Library. Das Skript ist ein lokales
Entwicklungswerkzeug mit festem Management-Endpunkt `127.0.0.1:15672`. Es erstellt
keine Benutzer oder Namespaces; die Demo-Werte dafür setzt Compose beim ersten
Start des frischen Volumes. Bei geänderten Zugangsdaten beide Konfigurationen
aufeinander abstimmen. Bestehende Volumes werden durch geänderte Docker-
Initialisierungsvariablen nicht automatisch migriert.

## 3. Was wird aus einem Topic oder Subject?

| Anwendungsbegriff | Konkrete RabbitMQ-Ressource |
|---|---|
| Namespace `demo` | Virtual Host `demo` |
| Topic `users` | Dauerhafte Topic-Exchange `phore.topic:users` |
| Subscription `audit-users` auf `users` | Quorum Queue `phore.sub:users:audit-users` |
| Nachrichtentyp / Subject `user.created.v1` | Routing Key `user.created.v1`, keine eigene anzulegende Ressource |
| Typfilter der Subscription | Binding zwischen Exchange und Queue |
| Fehlerablage dieser Subscription | Quorum Queue `phore.failure:users:audit-users` |

**Ein Subject muss nicht „eröffnet“ werden.** In diesem Entwurf ist damit die
Routingbezeichnung `type` gemeint. Es gibt bewusst keinen zweiten Parameter
`subject`. Beim Publish wird der Typ mitgeschickt. Die passende Subscription
muss bereits gebunden sein. `type: null` erzeugt intern ein `#`-Binding; exakte
Typnamen werden unverändert gebunden. Öffentliche Pattern-Filter gibt es zunächst nicht.

Zwei Subscription-Namen ergeben zwei Kopien. Drei Worker derselben Subscription
teilen deren Queue. Eine Exchange allein bewahrt keine Nachrichten auf:
Wird vor der ersten passenden Bindung gesendet, bekommt eine später angelegte
Subscription diese Nachricht nicht nachträglich. Die geplante Library lehnt
nicht routbare Publishes ausdrücklich ab. [RabbitMQ Exchanges](https://www.rabbitmq.com/docs/exchanges)

## 4. Dynamisch oder per Setup-Skript?

Beides ist möglich, aber nur das PHP-Skript ist bereits vorhanden:

| Vorgang | Bereits verwendbares Setup | Geplante PHP-Library |
|---|---|---|
| Fachliche Topologie vorher anlegen | Konfiguration bearbeiten, `setup.php` ausführen | Beim Start registrieren und mit `autoCreate: true` deklarieren |
| Bestehende Ressourcen weiterverwenden | Identische Deklarationen wiederholen | `autoCreate: false`, erwartete Topologie prüfen |
| Neues Subject verwenden | Exakten Filter ergänzen oder Subscription ohne Typfilter verwenden | `publish(topic, type, payload)`; keine eigene Subject-Ressource |
| Filter oder Queue-Eigenschaften ändern | Neue Subscription oder explizite Migration | Konflikt als Exception; keine automatische Migration |
| Ressourcen entfernen | Expliziter administrativer Eingriff | `cancel()` beendet nur den lokalen Consumer |

Entwurfsbeispiel nach Implementierung, mit der gemeinsamen Demo-Konfiguration:

```php
require_once __DIR__ . '/../examples/api-draft/connection.php';

$mq = new \Phore\MessageQueue\PhoreMQ(
    ...\Examples\MessageQueue\demoConnection()
);
try {
    $mq->subscribe('users', 'audit-users', function (array $event): void {
        echo $event['userId'];
    }, new \Phore\MessageQueue\SubscriptionOptions(type: 'user.created.v1'));

    // Die Binding-Anlage ist abgeschlossen, bevor die Nachricht veröffentlicht wird.
    $mq->publish('users', 'user.created.v1', ['userId' => 'u-1']);
    $mq->run(maxMessages: 1, maxSeconds: 5);
} finally {
    $mq->close();
}
```

Das Beispiel ist aus einem hypothetischen PHP-Skript im Verzeichnis `docs/`
referenziert; die vollständigen Beispieldateien verwenden ihre eigenen relativen Pfade.
Weitere Topics und Subscription-Namen in den Beispielen dürfen wegen des
expliziten Demo-`autoCreate` dynamisch entstehen. Die Topologieliste im JSON ist
kein Handler-Register; Callbacks werden weiterhin programmatisch oder über
Attribute registriert. Ein neues Topic im JSON startet keinen Worker.

Ohne `autoCreate` werden alle fachlichen Subscriptions einschließlich ihrer
internen Retry-/Fehlerressourcen vorab provisioniert. Das kleine PHP-Skript
zeigt die Basis- und Fehlerqueues; es provisioniert noch keine PhoreMQ-RPC-,
Health- oder Retry-Laufzeit. `rpc.enabled` erlaubt ausdrücklich die dynamischen
privaten Rückkanäle auch bei vorab angelegter fachlicher Topologie. Diese benötigen
eigene eingeschränkte Rechte. Health-Control-Ressourcen werden entsprechend der
expliziten Health-Konfiguration bereitgestellt; ein Check selbst legt nichts an.

**Nein, ein Konfigurationsskript ist technisch nicht zwingend:** RabbitMQ erlaubt
Deklarationen über AMQP während des Betriebs. Vorab-Provisionierung ist eine
Betriebsentscheidung. Bestehende Queues mit unvereinbaren Eigenschaften können
nicht einfach neu deklariert werden. [RabbitMQ Queue-Deklarationen](https://www.rabbitmq.com/docs/queues)

## 5. Prüfung und Grenzen

```bash
php deployment/rabbitmq/setup.php --dry-run
```

Der Dry Run validiert die Topologiedaten und zeigt die geplanten Operationen,
ohne Zugangsdaten auszugeben oder eine Verbindung aufzubauen. Beim echten Setup
führen HTTP-, Berechtigungs- und Deklarationsfehler zu einem Fehlerstatus. Bereits
erfolgreich angelegte Ressourcen bleiben bei Teilfehlern bestehen. Das Skript
löscht weder Nachrichten noch veraltete Bindungen; Filterwechsel benötigen eine
bewusste Migration. Identische Wiederholung ist zulässig.

Die Management-Oberfläche zeigt Queues und Consumer, beweist aber keine
fachliche Bereitschaft eines Dienstes. Dafür bleibt der standardisierte
PhoreMQ-Systemcheck vorgesehen. Die PHP-Beispiele brauchen später eine
Implementierung und einen laufenden Worker. Reine JSON-Nachrichten, die man
im Management-UI testweise veröffentlicht, sind noch keine gültigen signierten
PhoreMQ-Envelopes. [RabbitMQ Management](https://www.rabbitmq.com/docs/management)

## Publisher melden fehlende Initialisierung

Die automatische Anlage liegt beim Listener (`subscribe`/`respond`) oder beim
expliziten Setup. `publish` und `request` legen auch mit `autoCreate: true` keine
fachlichen Ressourcen an. Ein fehlendes Topic oder keine passende Subscription
führt direkt zu `QueueConfigurationMissingException` mit `reason`, `topic`
und `messageType`. Die Gründe sind `TOPIC_MISSING` und `NO_MATCHING_SUBSCRIPTION`.
Ein Beispiel zum Abfangen steht in [02-programmatic.php](../examples/api-draft/02-programmatic.php).

Die Oberfläche kann daraufhin anzeigen: „Die Nachrichtenverarbeitung ist noch
nicht eingerichtet. Möglicherweise fehlt die Initialisierung des zuständigen
Dienstes.“ Ein vorhandenes Queue-Ziel ohne laufenden Worker nimmt dagegen
weiterhin Nachrichten an; aktuelle Dienstbereitschaft wird mit `check()` geprüft.
Berechtigungsfehler und Verbindungsprobleme bleiben gesonderte Fehler.

## Queue-Profile und RPC in Containern (API-Entwurf)

[11-queue-options.php](../examples/api-draft/11-queue-options.php) zeigt
`QueueOptions::workQueue()`, `::rpc()` und `::broadcast()` sowie `#[Queue]` am DTO.
Globale `ConnectionOptions::queueDefaults` füllen offene Werte. Feste DTO-Werte
und explizite Subscription-Werte müssen übereinstimmen. Unvereinbare vorhandene
Konfiguration führt zu `QueueConfigurationConflictException`, nötige Neuerstellung
zu `QueueMigrationRequiredException`; nicht unterstützte Optionskombinationen
zu `UnsupportedQueueOptionException`. Kein automatisches Policy-Update, auch
nicht allein wegen einer höheren Revision. Diese Regeln gehören zur geplanten
Library; das kleine setup.php legt weiterhin nur seine dokumentierten dauerhaften
Basis-/Fehlerqueues an und verarbeitet keine neuen Profil- oder Revisionsfelder.

Work/RPC speichern Aufträge dauerhaft; maxAttempts zählt den Erstversuch mit,
retryDelaySeconds ist die feste Wartezeit zwischen regulären Retries (Default
vier Versuche, jeweils zehn Sekunden). Flüchtiger Broadcast verteilt an aktuell
registrierte Verbindungen ohne Offline-Garantie. Mit positiver Retention erhält
jede stabile Empfängergruppe eine dauerhafte Subscription; Ack entfernt ihre
Kopie früher. maxInFlight begrenzt offene Zustellungen je Consumer, startet keine
zusätzlichen Worker. Beispiele verwenden weiterhin dieselbe zentrale Verbindung.

[05-rpc.php](../examples/api-draft/05-rpc.php) erklärt Publisher und Subscriber
als getrennte Container. Vor dem Request wird pro Publisher-Verbindung eine
private Reply-Queue samt Consumer eingerichtet; Request-ID und replyTo ordnen
Antworten zu. Mehrere Publisher teilen diese Queue nicht. Ein Timeout beendet
nur das Warten. close bzw. erkannter Verbindungsverlust entfernt die private
Queue samt Binding; die gemeinsame interne Exchange bleibt bestehen.
Ein neuer Container bekommt einen neuen Rückkanal und übernimmt keine offenen
Aufrufe. Für das Wiederaufnehmen schreibender Operationen müssen Vorgangs-ID und
Ergebnis außerhalb des Containers atomar gespeichert werden. Vollständige
Fehlerfenster und Cleanup-Limits stehen im Proposal §§ 13.2 und 13.6.

Container im Compose-Netz verwenden den Dienstnamen rabbitmq statt 127.0.0.1;
die zentrale Connection muss dann `amqp://demo:demo@rabbitmq:5672/demo` und
managementUrl `http://rabbitmq:15672` enthalten. Der lokale setup.php-Aufruf läuft
weiter auf dem Host mit seiner Host-Konfiguration. Es werden nur zwei einzelne
Ports veröffentlicht, keine Port-Range: 5672 für AMQP und 15672 für Management.
