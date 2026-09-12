<?php

declare(strict_types=1);

namespace Examples\MessageQueue\Attributes;

use Phore\MessageQueue\Attribute\MessageType;
use Phore\MessageQueue\Attribute\Subscribe;
use Phore\MessageQueue\ConnectionFactory;
use Phore\MessageQueue\ConnectionOptions;
use Phore\MessageQueue\MessageContext;
use Phore\MessageQueue\MessageQueueInterface;
use Phore\MessageQueue\RunOptions;
use Phore\MessageQueue\Schema\PhoreSchemaMapper;
use Phore\MessageQueue\Security\HmacSecurity;

/**
 * API-ENTWURF, noch nicht ausführbar. Proposal §§ 4–6.
 * Für ein echtes SDK würde T_UserCreated in einer eigenen Composer-Library
 * liegen. Hier bleiben alle Teile zum Lesen in einer Beispieldatei.
 */

#[MessageType('user.created.v1', topic: 'users')]
final class T_UserCreated
{
    public string $userId;
    public string $email;
}

final class UserHandlers
{
    // Topic/Subscription stehen am Handler, die Klasse ergibt sich per Reflection.
    #[Subscribe(topic: 'users', subscription: 'sdk-users', type: 'user.created.v1')]
    public function onCreated(T_UserCreated $user, MessageContext $context): void
    {
        printf("SDK: %s / %s\n", $context->messageId, $user->email);
        // Erfolgreiche Rückkehr bestätigt automatisch.
    }

    // Auch Attribute erzwingen keine Typisierung: hier unveränderte Array-Daten.
    #[Subscribe(topic: 'users', subscription: 'raw-users', type: 'user.created.v1')]
    public function onRawCreated(array $data): void
    {
        printf("Array: %s\n", json_encode($data, JSON_THROW_ON_ERROR));
    }
}

function createConnection(string $dsn, string $sharedSecret): MessageQueueInterface
{
    return (new ConnectionFactory())->connect($dsn, new ConnectionOptions(
        security: new HmacSecurity(
            sharedSecret: $sharedSecret,
            keyId: 'development-1',
            audience: 'user-services-development',
        ),
        schemaMapper: new PhoreSchemaMapper(),
        autoCreate: true,
    ));
}

function send(MessageQueueInterface $mq): void
{
    $user = new T_UserCreated();
    $user->userId = 'u-789';
    $user->email = 'sdk-user@example.org';

    $mq->emit($user); // Liest MessageType, validiert und serialisiert.

    // Gleichwertige explizite API, etwa für eine andere Anwendung ohne SDK:
    $mq->publish('users', 'user.created.v1', [
        'userId' => 'u-790',
        'email' => 'manual@example.org',
    ]);
}

function demo(string $dsn, string $sharedSecret): void
{
    $mq = createConnection($dsn, $sharedSecret);
    try {
        // Explizite Registrierung, keine Magie und kein Container erforderlich.
        $mq->registerHandlers(new UserHandlers());
        send($mq);
        $mq->run(new RunOptions(maxMessages: 4, maxSeconds: 10));
    } finally {
        $mq->close();
    }
}

// Getrennte Prozesse: Empfänger legt/bindet Subscriptions vor dem ersten Senden
// an und ruft run() auf. Sender ruft danach send() auf seiner eigenen Connection
// auf. Beide verwenden denselben Broker-Prefix, HMAC-Key und dieselbe Audience.
