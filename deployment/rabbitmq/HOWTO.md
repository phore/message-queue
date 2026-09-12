# RabbitMQ im internen Docker-Netz

Dieses Beispiel startet RabbitMQ 4.3 ohne Management-Plugin und ohne Portfreigabe
am Host. Compose erstellt Netzwerk und Datenvolume; RabbitMQ initialisiert Benutzer,
Passwort und vHost beim ersten Start automatisch. Es braucht weder Webkonsole noch
manuelle rabbitmqctl-Befehle. Die bekannten Zugangsdaten sind Demo-Werte.

## Start mit automatisch vorgegebenen Zugangsdaten

Aus dem Repository-Verzeichnis:

```bash
docker compose -p phore-internal -f deployment/rabbitmq/compose.internal.yaml up -d --wait
```

Dienste im selben Netzwerk verbinden sich über
`amqp://mq-demo:mq-demo-password@rabbitmq:5672/app`.
`rabbitmq` ist der Service-DNS-Name; feste IPs und localhost sind dafür falsch.
Der Healthcheck prüft den laufenden Broker, nicht die Bereitschaft fachlicher Handler.

## Ohne Benutzername und Passwort im Client

**Ja, mit RabbitMQ 4.3 und SASL ANONYMOUS.** Der Broker ordnet anonyme Clients
intern dem Benutzer aus anonymous_login_user/anonymous_login_pass zu. Es werden
also keine Client-Credentials übertragen, aber Berechtigungen gehören weiterhin
zu einer Broker-Identität. Alle anonymen Clients teilen diese Rechte; das ist keine
Identifikation einzelner Dienste. Diese Variante ist für das isolierte Testnetz.
[RabbitMQ Authentication](https://www.rabbitmq.com/docs/access-control#authentication-mechanisms)

```bash
docker compose -p phore-anonymous -f deployment/rabbitmq/compose.internal.yaml -f deployment/rabbitmq/compose.anonymous.yaml up -d --wait
```

Die zweite Datei ersetzt nur den Konfigurationsmount. Sie aktiviert ANONYMOUS und
ordnet die Clients dem automatisch angelegten mq-demo-Benutzer zu. Der separate
Projektname gibt dieser Demo ein eigenes Netzwerk und Volume. Broker-Ziel für den
Client: Host rabbitmq, Port 5672, vHost app, Mechanismus ANONYMOUS; **keine**
Username-/Password-Felder. Eine URL ohne Credentials allein schaltet einen Client
nicht auf ANONYMOUS um: Viele Clients wählen sonst PLAIN mit Standardwerten.
Die PHP-AMQP-Clientbibliothek muss diesen Mechanismus ausdrücklich unterstützen.

Die Einstellungen wurden zusätzlich am
[RabbitMQ-4.3.0-Mechanismus](https://github.com/rabbitmq/rabbitmq-server/blob/v4.3.0/deps/rabbit/src/rabbit_auth_mechanism_anonymous.erl)
und am [Konfigurationsschema](https://github.com/rabbitmq/rabbitmq-server/blob/v4.3.0/deps/rabbit/priv/schema/rabbit.schema)
abgeglichen. Kein guest-Remote-Login und kein leeres Passwort werden als Ersatz
für echte anonyme Anmeldung verwendet.

## In eine bestehende Compose-Anwendung übernehmen

Übernimm den rabbitmq-Service, das mq-Netzwerk und das data-Volume aus
compose.internal.yaml in deine bestehende Datei; kopiere internal.conf daneben
und passe den Mountpfad an. Ergänze bei den bestehenden Diensten:

```yaml
services:
  publisher:
    # Vorhandene image/build/command-Angaben deines Dienstes bleiben hier stehen.
    networks: [default, mq]
    depends_on:
      rabbitmq:
        condition: service_healthy
  subscriber:
    # Vorhandene image/build/command-Angaben deines Workers bleiben hier stehen.
    networks: [mq]
    depends_on:
      rabbitmq:
        condition: service_healthy
networks:
  default: {}
  mq:
    driver: bridge
    internal: true
```

Dies ist ein Integrationsausschnitt, kein eigenständig startbares Publisher-Image.
Die MQ-Library und die fachlichen Container sind in diesem Repository noch nicht
implementiert. Übergib die oben genannte Verbindung über die vorhandene
Anwendungskonfiguration; Compose allein konfiguriert keine PHP-Library.
Benötigt der Worker externe APIs, erhält auch er zusätzlich ein geeignetes Netz.
internal isoliert das mq-Netz nach außen, ist aber keine Zugriffskontrolle zwischen
seinen Mitgliedern. Ein Dienst mit zwei Netzen kann selbst Verbindungen vermitteln.
[Docker Compose networks](https://docs.docker.com/reference/compose-file/networks/)

Für zwei **getrennte Compose-Projekte** kann das zweite das vom ersten erzeugte
Netz nutzen, nachdem der Broker gestartet wurde:

```yaml
networks:
  mq:
    external: true
    name: phore-internal_mq
```

Für die anonyme Variante heißt es phore-anonymous_mq. Das externe Netz wird durch
das erste Projekt angelegt; external bedeutet „bereits vorhanden“, nicht
„öffentlich“. Beim externen Verweis kein zusätzliches internal/driver setzen.
Projektübergreifend gibt es kein depends_on: Clients brauchen Start-Retries.
Ohne manuelle Netzwerkerstellung ist die gemeinsame Compose-Datei am einfachsten.

## Was bedeutet DEFAULT_VHOST?

Ein vHost trennt Queues, Exchanges, Bindings und Berechtigungen logisch innerhalb
desselben Brokers. RABBITMQ_DEFAULT_VHOST=app legt beim ersten Start der leeren
Broker-Datenbank den vHost app an. Der initialisierte Default-Benutzer erhält dort
Zugriff. Ohne Anpassung heißt der RabbitMQ-Standard-vHost `/`.
[RabbitMQ Virtual Hosts](https://www.rabbitmq.com/docs/vhosts)

Der Client wählt seinen vHost bei der Verbindung ausdrücklich: `/app` im DSN
bedeutet app; `/%2F` bedeutet den vHost mit Namen `/`. DEFAULT_VHOST leitet Clients
nicht automatisch um und erzeugt keine fachlichen Topics oder Subscriber.
Ein vHost ist weder Netzwerk noch eigener Port. Derselbe Port kann viele vHosts
bedienen. Alle zusammenarbeitenden Publisher und Subscriber müssen denselben wählen.

## Welche Ports müssen freigegeben werden?

| Verbindung | Einstellung in diesem Beispiel |
|---|---|
| Dienst → Broker im mq-Netz | rabbitmq:5672/TCP; kein ports oder expose nötig |
| Host oder Rechner im LAN → Broker | Keine Veröffentlichung, kein regulärer Zugang über einen Host-Port |
| Management-Webkonsole / HTTP-API | Nicht installiert, kein Listener auf 15672 |
| TLS-AMQP auf 5671 | Nicht konfiguriert; TLS benötigt eigene Zertifikats-/Listener-Konfiguration |
| Cluster-/CLI-Kommunikation 4369/25672 | Nicht veröffentlichen; ein Knoten, CLI per docker compose exec |

ports würde einen Container-Port am Host veröffentlichen; expose dokumentiert
Container-Ports, ist aber keine Firewall und keine Voraussetzung für Kommunikation
im gemeinsamen Netz. „Lokales Netzwerk“ meint hier das Docker-Bridge-Netz auf
**einem** Docker-Host, nicht automatisch das LAN zwischen mehreren Hosts.
Soll später bewusst ein Hostzugang entstehen, wäre `127.0.0.1:5672:5672` ausschließlich
lokal. Für LAN-Zugriff wären Bind-Adresse, Firewall und Authentifizierung neu zu
konfigurieren; das anonyme Beispiel veröffentlicht absichtlich keinen Port.
[RabbitMQ Networking](https://www.rabbitmq.com/docs/networking)

## Volume, Neustarts und Konfigurationsänderungen

Das benannte Volume data wird unter /var/lib/rabbitmq eingebunden. Der feste
Hostname hält den RabbitMQ-Knotennamen bei Container-Neuerstellung stabil.
Dauerhafte Queues und persistente Nachrichten können dadurch Neustarts überstehen;
flüchtige RPC-Rückkanäle bleiben flüchtig. Ein einzelner Knoten bietet keine HA.

```bash
docker compose -p phore-internal -f deployment/rabbitmq/compose.internal.yaml restart
docker compose -p phore-internal -f deployment/rabbitmq/compose.internal.yaml exec rabbitmq rabbitmqctl list_vhosts
docker compose -p phore-internal -f deployment/rabbitmq/compose.internal.yaml exec rabbitmq rabbitmqctl list_queues -p app name messages consumers
```

DEFAULT_USER/PASS/VHOST initialisieren ausschließlich eine leere Datenbank. Änderungen
an diesen Variablen migrieren ein bestehendes Volume nicht. Beim anonymen Modus
müssen die internen anonymous_login-Werte zum gespeicherten Benutzer passen.
`down` erhält das Volume, `down -v` löscht es mitsamt Nachrichten und Einstellungen.
Für das anonyme Projekt bei Verwaltungsbefehlen denselben Projektnamen und beide
Compose-Dateien wie beim Start verwenden.

## Grenze zum aktuellen PhoreMQ-Entwurf

RabbitMQ kann Topics/Queues/Bindings über AMQP ohne Management-Plugin anlegen.
Die geplante PhoreMQ-Library verlangt für ihre **vollständige** Topologieprüfung
jedoch derzeit eine Management-HTTP-API über managementUrl. Diese Broker-Demo
ist deshalb noch kein vollständig kompatibles PhoreMQ-End-to-End-Deployment:
ohne diese Prüfmöglichkeit ist TopologyVerificationException vorgesehen.
Auch ein expliziter ANONYMOUS-Clientmodus ist in der bisherigen PhoreMQ-API noch
nicht festgelegt. Dieses Deployment ändert diese Verträge nicht stillschweigend.

Für den bisherigen Entwurf bleibt compose.yaml mit Management-Plugin verwendbar;
für rein interne Nutzung können dort die ports-Einträge entfallen, während die API
im Container-Netz erreichbar bleibt. Eine manuelle Bedienung der Konsole ist nicht
nötig. setup.php ist ein separates Host-Werkzeug für diese Management-Demo und wird
im neuen internen Beispiel nicht ausgeführt. Fachliche Topologie ist dadurch beim
Brokerstart noch nicht angelegt; das übernimmt später der autorisierte Listener.
