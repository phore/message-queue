#!/usr/bin/env python3
"""Provision the local RabbitMQ demo from neutral config; no PhoreMQ runtime needed.

Repeated identical declarations are safe. No resources or messages are deleted.
Only local HTTP management is supported by this development helper.
"""
import argparse
import base64
import json
import re
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urlsplit
from urllib.request import Request, build_opener, ProxyHandler, HTTPRedirectHandler


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def declarations(config):
    if set(config) != {"connection", "options", "topics", "subscriptions"}:
        raise ValueError("Expected connection, options, topics and subscriptions")
    if not isinstance(config["topics"], list) or not isinstance(config["subscriptions"], list):
        raise ValueError("topics and subscriptions must be lists")
    name = re.compile(r"[a-zA-Z_][a-zA-Z0-9_.-]{0,99}\Z")
    topics = config["topics"]
    if any(not isinstance(t, str) or (not name.fullmatch(t) or t.startswith("_phore")) for t in topics):
        raise ValueError("Invalid topic name")
    if len(set(topics)) != len(topics):
        raise ValueError("Duplicate topic")
    connection = urlsplit(config["connection"])
    if connection.scheme != "amqp" or connection.hostname not in ("127.0.0.1", "localhost"):
        raise ValueError("This helper requires a local amqp DSN")
    if connection.port != 5672 or connection.query or connection.fragment or not connection.username or connection.password is None:
        raise ValueError("Invalid local connection DSN")
    if not connection.path.startswith("/") or not connection.path[1:]:
        raise ValueError("DSN must include a namespace")
    namespace = unquote(connection.path[1:])
    vhost = quote(namespace, safe="")
    actions = []
    for topic in topics:
        actions.append(("PUT", f"exchanges/{vhost}/{quote('phore.topic:' + topic, safe='')}",
                        {"type": "topic", "durable": True, "auto_delete": False, "internal": False, "arguments": {}}))
    seen = set()
    for sub in config["subscriptions"]:
        if not isinstance(sub, dict) or set(sub) != {"topic", "name", "type"}:
            raise ValueError("Subscription requires topic, name and type (null means all)")
        topic, subscription, kind = sub["topic"], sub["name"], sub["type"]
        if topic not in topics or not isinstance(subscription, str) or not name.fullmatch(subscription):
            raise ValueError("Invalid subscription or unknown topic")
        if kind is not None and (not isinstance(kind, str) or not name.fullmatch(kind)):
            raise ValueError("type must be an exact message type or null")
        if (topic, subscription) in seen:
            raise ValueError("Duplicate subscription")
        seen.add((topic, subscription))
        queue = f"phore.sub:{topic}:{subscription}"
        failure = f"phore.failure:{topic}:{subscription}"
        args = {"x-queue-type": "quorum"}
        actions.append(("PUT", f"queues/{vhost}/{quote(failure, safe='')}",
                        {"durable": True, "auto_delete": False, "arguments": args}))
        # A dedicated failure destination never fans failed work out to other subscriptions.
        args = {"x-queue-type": "quorum", "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": failure, "x-overflow": "reject-publish",
                "x-dead-letter-strategy": "at-least-once", "x-delivery-limit": -1}
        actions.append(("PUT", f"queues/{vhost}/{quote(queue, safe='')}",
                        {"durable": True, "auto_delete": False, "arguments": args}))
        actions.append(("POST", f"bindings/{vhost}/e/{quote('phore.topic:' + topic, safe='')}/q/{quote(queue, safe='')}",
                        {"routing_key": kind if kind is not None else "#", "arguments": {}}))
    return connection, actions


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path(__file__).resolve().parents[2] / "config/message-queue.json")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print declarations without connecting")
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    connection, actions = declarations(config)  # Validate everything before the first write.
    if args.dry_run:
        print(json.dumps(actions, indent=2))  # Contains no DSN or credentials.
        return
    credentials = f"{unquote(connection.username)}:{unquote(connection.password)}".encode()
    headers = {"Authorization": "Basic " + base64.b64encode(credentials).decode(), "Content-Type": "application/json"}
    opener = build_opener(ProxyHandler({}), NoRedirect())
    base = "http://127.0.0.1:15672/api/"
    # This fixed loopback endpoint prevents config from redirecting demo credentials.
    for method, path, body in actions:
        if method == "POST":
            with opener.open(Request(base + path, headers=headers), timeout=10) as response:
                existing = json.load(response)
            if existing and any(b["routing_key"] != body["routing_key"] or b.get("arguments") for b in existing):
                raise ValueError("Existing subscription filter differs; use a new subscription or an explicit migration")
        with opener.open(Request(base + path, json.dumps(body).encode(), headers, method=method), timeout=10):
            pass
    print(f"Topology ready: {len(config['topics'])} topics, {len(config['subscriptions'])} subscriptions")


if __name__ == "__main__":
    try:
        main()
    except HTTPError as error:
        raise SystemExit(f"RabbitMQ management HTTP {error.code}; check readiness, permissions and declaration conflicts") from None
    except (ValueError, TypeError, KeyError, OSError, URLError) as error:
        raise SystemExit(f"Setup failed: {error}") from None
