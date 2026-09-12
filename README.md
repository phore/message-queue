# Phore Message Queue

Geplante universelle PHP-Library für Message Queues mit austauschbaren
Konnektoren. Redis Streams bildet die erste Implementierung; eine gemeinsame
API verbindet Topics, dauerhafte Subscriptions und konkurrierende Worker.
Optionale strukturelle Validierung und DTO-Hydration verwenden `phore/schema`
aus dem Repository `phore/phore-schema`. Nachrichtentypen und Handler lassen
sich programmatisch oder über PHP-Attribute zuordnen. Eine austauschbare
Sicherheitsschicht signiert Nachrichten transparent mit HMAC-SHA-256;
große Dateien werden über verifizierte Speicherreferenzen transportiert.

**Status: API-Entwurf, noch keine Queue-Implementierung.** Composer-Metadaten
und Autoloading stammen weiterhin aus der Projektvorlage. Die folgenden
PHP-Dateien zeigen die vorgeschlagene API und sind noch nicht ausführbar.

- [API-Entwurf, Konnektorvergleich und Paketgrenzen](docs/proposals/2026-09-12-message-queue-api.md)
- [Verbinden: URL, Konnektor und Attribute](examples/api-draft/01-connect.php)
- [Programmatisch senden und empfangen](examples/api-draft/02-programmatic.php)
- [SDK-Typen und Handler mit Attributen](examples/api-draft/03-attributes.php)
- [ZIP-Dateien und lokale Entwicklung](examples/api-draft/04-files-and-local.php)

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

