# Projektregeln für phore/message-queue

- Mindestversion ist PHP 8.5 (`>=8.5`), für Library-Code, Tests, Beispiele und ausführbare Setup-/Hilfsskripte. Keine Kompatibilitätsschichten für ältere PHP-Versionen vorsehen.
- Ausführbare Beispiele und Setup-/Hilfsskripte werden ausschließlich in PHP geschrieben. Dokumentationsbeispiele verwenden ebenfalls PHP. Deklarative JSON-/YAML-Konfiguration und Shell-Befehle zum Aufrufen von PHP, Composer und Docker bleiben zulässig.
- Zunächst wird ausschließlich RabbitMQ über einen Adapter hinter `ConnectorInterface` umgesetzt. Keine Adapterregistrierung, Brokerauswahl oder Fallback-Logik; öffentliche Begriffe bleiben Namespace, Topic, Subscription und Nachrichtentyp.
- `docs/setup.md`, `examples/` und die Deployment-Dateien werden gemeinsam mit den zugehörigen Änderungen im Repository gepflegt. Noch nicht implementierte PHP-APIs sind ausdrücklich als Entwurf zu kennzeichnen.
- Maßgeblicher Architekturentwurf: `docs/proposals/2026-09-12-message-queue-api.md`. Die Projektübersicht für Agenten steht in `.ai-usage-info.md`.
