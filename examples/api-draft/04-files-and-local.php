<?php

declare(strict_types=1);

namespace Examples\MessageQueue\FilesAndLocal;

require_once __DIR__ . '/connection.php';

use function Examples\MessageQueue\demoConnection;

use Phore\MessageQueue\PhoreMQ;
use Phore\MessageQueue\Attachment;
use Phore\MessageQueue\ConnectionOptions;
use Phore\MessageQueue\PayloadStoreInterface;
use Phore\MessageQueue\MessageContext;
use Phore\MessageQueue\PublishOptions;
use Phore\MessageQueue\RunOptions;
use Phore\MessageQueue\SubscriptionOptions;

/**
 * API-ENTWURF, noch nicht ausführbar. Proposal §§ 8–10.
 * ZIP-Transport per Dateireferenz; RabbitMQ-Verbindung steht in 01-connect.php.
 * Eingabe-/Ausgabepfad kommen vom Aufrufer, keine Environment-Reads.
 */

function zipDemo(string $zipPath, string $outputPath, PayloadStoreInterface $payloadStore): void
{
    $mq = new PhoreMQ(...demoConnection(new ConnectionOptions(payloadStore: $payloadStore)));
    // Ein von Sender und Empfänger erreichbarer Dateispeicher wird ausdrücklich injiziert.
    // RabbitMQ transportiert nur die verifizierte Referenz; kein eingebauter Dateispeicher.

    try {
        $mq->subscribe('exports', 'archive-importer', function (array $data, MessageContext $context) use ($outputPath): void {
            // Verifiziert Größe und Digest vor Freigabe des Ziels; kein Entpacken.
            // Ziel kommt aus lokaler Konfiguration, niemals aus dem Dateinamen.
            $context->attachment('archive')->copyTo($outputPath);
            printf("Export %s liegt verifiziert bereit.\n", $data['exportId']);
        }, new SubscriptionOptions(type: 'export.ready.v1'));

        $mq->publish('exports', 'export.ready.v1', ['exportId' => 'export-42'], new PublishOptions(
            attachments: [
                'archive' => Attachment::fromPath($zipPath, contentType: 'application/zip'),
            ],
        ));
        // Höchstens 1 Zustellversuch(e) insgesamt oder 30 s Gesamtbudget; erstes Limit gewinnt.
        // Normale Rückkehr, keine Mindestzahl/Timeout-Exception; Details in 02-programmatic.php.
        $mq->run(new RunOptions(maxMessages: 1, maxSeconds: 30));
    } finally {
        $mq->close();
    }
}
