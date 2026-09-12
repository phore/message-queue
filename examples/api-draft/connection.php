<?php

declare(strict_types=1);

namespace Examples\MessageQueue;

use Phore\MessageQueue\ConnectionOptions;

/**
 * API-ENTWURF: ConnectionOptions::fromArray ist noch nicht implementiert.
 * Einmalige Demo-Konfiguration für alle Beispiele; keine Environment-Reads.
 * Explizite Felder in $overrides ergänzen/überschreiben die Basisoptionen;
 * ausgelassene Felder behalten die Werte aus der Konfigurationsdatei.
 * JSON darf nur dokumentierte Optionswerte enthalten, keine PHP-Klassennamen.
 * Die Topologielisten liest setup.py; autoCreate erlaubt zusätzliche Demo-Bindungen.
 * @return array{string, ConnectionOptions}
 */
function demoConnection(?ConnectionOptions $overrides = null): array
{
    $path = __DIR__ . '/../../config/message-queue.json';
    $json = file_get_contents($path);
    if ($json === false) {
        throw new \RuntimeException('Die Demo-Konfiguration konnte nicht gelesen werden.');
    }
    $config = json_decode($json, true, 512, JSON_THROW_ON_ERROR);

    return [$config['connection'], ConnectionOptions::fromArray($config['options'], overrides: $overrides)];
}
