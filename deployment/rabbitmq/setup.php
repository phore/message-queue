<?php

declare(strict_types=1);

namespace Phore\MessageQueue\Deployment;

// Ausführbares CLI-Beispiel, unabhängig von der noch nicht implementierten Library.
// Aufruf: php deployment/rabbitmq/setup.php [--config DATEI] [--dry-run]
if (PHP_VERSION_ID < 80500) {
    throw new \RuntimeException('Dieses Projekt benötigt PHP >= 8.5.');
}

/** @return array{array, array} Verbindung und vollständig validierte Deklarationen. */
function declarations(#[\SensitiveParameter] array $config): array
{
    $keys = array_keys($config);
    sort($keys);
    if ($keys !== ['connection', 'options', 'subscriptions', 'topics']
        || !is_string($config['connection'])
        || !is_array($config['topics']) || !array_is_list($config['topics'])
        || !is_array($config['subscriptions']) || !array_is_list($config['subscriptions'])) {
        throw new \InvalidArgumentException('Erwartet: connection, options, topics und subscriptions.');
    }
    $validName = static fn (mixed $name): bool => is_string($name)
        && preg_match('/\A[a-zA-Z_][a-zA-Z0-9_.-]{0,99}\z/', $name) === 1;
    $topics = $config['topics'];
    foreach ($topics as $topic) {
        if (!$validName($topic) || str_starts_with($topic, '_phore')) {
            throw new \InvalidArgumentException('Ungültiger oder reservierter Topic-Name.');
        }
    }
    if (count(array_unique($topics)) !== count($topics)) {
        throw new \InvalidArgumentException('Doppeltes Topic.');
    }
    $connection = parse_url($config['connection']);
    if ($connection === false || ($connection['scheme'] ?? null) !== 'amqp'
        || !in_array($connection['host'] ?? null, ['127.0.0.1', 'localhost'], true)
        || ($connection['port'] ?? null) !== 5672
        || isset($connection['query']) || isset($connection['fragment'])
        || ($connection['user'] ?? '') === '' || !isset($connection['pass'])
        || !str_starts_with($connection['path'] ?? '', '/') || strlen($connection['path']) < 2) {
        throw new \InvalidArgumentException('Lokale AMQP-DSN mit Port 5672, Credentials und Namespace erforderlich.');
    }
    $vhost = rawurlencode(rawurldecode(substr($connection['path'], 1)));
    $actions = [];
    foreach ($topics as $topic) {
        $actions[] = ['PUT', "exchanges/$vhost/" . rawurlencode('phore.topic:' . $topic), [
            'type' => 'topic', 'durable' => true, 'auto_delete' => false,
            'internal' => false, 'arguments' => (object) [],
        ]];
    }
    $seen = [];
    foreach ($config['subscriptions'] as $sub) {
        if (!is_array($sub)) {
            throw new \InvalidArgumentException('Subscription muss ein Objekt sein.');
        }
        $keys = array_keys($sub);
        sort($keys);
        if ($keys !== ['name', 'topic', 'type']) {
            throw new \InvalidArgumentException('Subscription benötigt topic, name und type (null für alle).');
        }
        ['topic' => $topic, 'name' => $name, 'type' => $type] = $sub;
        if (!in_array($topic, $topics, true) || !$validName($name)
            || ($type !== null && !$validName($type))) {
            throw new \InvalidArgumentException('Ungültige Subscription, unbekanntes Topic oder ungültiger Typfilter.');
        }
        $id = "$topic:$name";
        if (isset($seen[$id])) {
            throw new \InvalidArgumentException('Doppelte Subscription.');
        }
        $seen[$id] = true;
        $queue = "phore.sub:$id";
        $failure = "phore.failure:$id";
        $actions[] = ['PUT', "queues/$vhost/" . rawurlencode($failure), [
            'durable' => true, 'auto_delete' => false, 'arguments' => ['x-queue-type' => 'quorum'],
        ]];
        // Fehler gehen ausschließlich an diese Subscription, niemals erneut als Fan-out.
        $actions[] = ['PUT', "queues/$vhost/" . rawurlencode($queue), [
            'durable' => true, 'auto_delete' => false, 'arguments' => [
                'x-queue-type' => 'quorum', 'x-dead-letter-exchange' => '',
                'x-dead-letter-routing-key' => $failure, 'x-overflow' => 'reject-publish',
                'x-dead-letter-strategy' => 'at-least-once', 'x-delivery-limit' => -1,
            ],
        ]];
        $actions[] = ['POST', "bindings/$vhost/e/" . rawurlencode('phore.topic:' . $topic)
            . '/q/' . rawurlencode($queue), ['routing_key' => $type ?? '#', 'arguments' => (object) []]];
    }
    return [$connection, $actions];
}

function managementRequest(string $method, string $path, #[\SensitiveParameter] string $authorization, ?array $body = null): string
{
    $context = stream_context_create(['http' => [
        'method' => $method,
        'header' => "Authorization: Basic $authorization\r\nContent-Type: application/json\r\nConnection: close\r\n",
        'content' => $body === null ? '' : json_encode($body, JSON_THROW_ON_ERROR),
        'timeout' => 10,
        'ignore_errors' => true,
        'follow_location' => 0,
        'max_redirects' => 0,
    ]]);
    // Sicherheitsgrenze: festes Loopback-Ziel, keine Redirects und keine URL aus Payloads.
    // Keine Credentials in URLs oder Fehlermeldungen; Antwortgröße ist begrenzt.
    http_clear_last_response_headers();
    $result = @file_get_contents('http://127.0.0.1:15672/api/' . $path, false, $context, 0, 1048577);
    $headers = http_get_last_response_headers() ?? [];
    if ($result === false) {
        throw new \RuntimeException('Management-Verbindung fehlgeschlagen; Broker und allow_url_fopen prüfen.');
    }
    if (strlen($result) > 1048576) {
        throw new \RuntimeException('Management-Antwort überschreitet das Größenlimit.');
    }
    $status = 0;
    foreach ($headers as $header) {
        if (preg_match('/\AHTTP\/\S+\s+(\d{3})\b/', $header, $match) === 1) {
            $status = (int) $match[1];
        }
    }
    if ($status < 200 || $status >= 300) {
        throw new \RuntimeException("RabbitMQ Management HTTP $status; Bereitschaft, Rechte und Deklarationskonflikte prüfen.");
    }
    return $result;
}

function main(array $arguments): void
{
    $path = __DIR__ . '/../../config/message-queue.json';
    $dryRun = false;
    for ($i = 0; $i < count($arguments); $i++) {
        if ($arguments[$i] === '--dry-run') {
            $dryRun = true;
        } elseif ($arguments[$i] === '--config' && isset($arguments[$i + 1])) {
            $path = $arguments[++$i];
        } else {
            throw new \InvalidArgumentException('Aufruf: php setup.php [--config DATEI] [--dry-run]');
        }
    }
    $json = @file_get_contents($path);
    if ($json === false) {
        throw new \RuntimeException('Konfigurationsdatei konnte nicht gelesen werden.');
    }
    $config = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
    if (!is_array($config)) {
        throw new \InvalidArgumentException('Konfiguration muss ein JSON-Objekt sein.');
    }
    [$connection, $actions] = declarations($config); // Alles vor dem ersten Schreibzugriff prüfen.
    if ($dryRun) {
        echo json_encode($actions, JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR), "\n";
        return; // Dry Run enthält weder DSN noch Zugangsdaten.
    }
    $authorization = base64_encode(rawurldecode($connection['user']) . ':' . rawurldecode($connection['pass']));
    foreach ($actions as [$method, $resource, $body]) {
        if ($method === 'POST') {
            $bindings = json_decode(managementRequest('GET', $resource, $authorization), true, 512, JSON_THROW_ON_ERROR);
            if (!is_array($bindings) || !array_is_list($bindings)) {
                throw new \RuntimeException('Ungültige Binding-Antwort.');
            }
            foreach ($bindings as $binding) {
                if (!is_array($binding) || ($binding['routing_key'] ?? null) !== $body['routing_key']
                    || ($binding['arguments'] ?? null) !== []) {
                    throw new \RuntimeException('Bestehender Typfilter weicht ab; neue Subscription oder explizite Migration erforderlich.');
                }
            }
        }
        managementRequest($method, $resource, $authorization, $body);
    }
    printf("Topologie bereit: %d Topics, %d Subscriptions\n", count($config['topics']), count($config['subscriptions']));
}

// Ungefangene Exception liefert dem CLI einen Fehlerstatus; keine stillen Fehler.
if (PHP_SAPI === 'cli' && realpath($_SERVER['SCRIPT_FILENAME'] ?? '') === __FILE__) {
    main(array_slice($argv, 1));
}
