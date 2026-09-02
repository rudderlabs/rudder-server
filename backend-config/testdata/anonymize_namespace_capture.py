#!/usr/bin/env python3
"""Anonymize a paired v1/v2 namespace config capture.

Both files describe the same namespace, so every mapping is keyed purely on the
*original value*: the same workspace/source/destination id, name or config value
is rewritten to the same replacement in both files, which is what makes them
usable together for verifying the v2 -> v1 mapping.

Policy:
  * definitions (source/destination/account) are product metadata, not customer
    data: preserved verbatim, including their ids, so the join by definition
    name/id stays verifiable.
  * identifiers keep their shape (27-char ksuid, 20-char xid, uuid).
  * timestamps, numbers, booleans, empty strings and structural enums are kept.
  * every other string - entity names, descriptions and all config/secret
    values - is replaced with a deterministic, length-preserving filler.
"""

import hashlib
import json
import re
import string
import subprocess
import sys

B62 = string.digits + string.ascii_uppercase + string.ascii_lowercase
B32 = "0123456789abcdefghijklmnopqrstuv"
HEX = "0123456789abcdef"

KSUID = re.compile(r"^[0-9A-Za-z]{27}$")
PREFIXED_KSUID = re.compile(r"^([a-z]{1,6}_)([0-9A-Za-z]{27})$")
XID = re.compile(r"^[0-9a-v]{20}$")
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
ISO = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}")
NUMERIC = re.compile(r"^-?\d+(\.\d+)?$")
CLOCK = re.compile(r"^\d{1,2}:\d{2}(:\d{2})?$")
URL = re.compile(r"^(https?|wss?|s3|gs)://", re.I)
EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")

# Subtrees that are product metadata and stay verbatim.
DEFINITION_KEYS = {
    "sourceDefinitions",
    "destinationDefinitions",
    "accountDefinitions",
    "sourceDefinition",
    "destinationDefinition",
    "accountDefinition",
}

# Leaf keys whose values are structural enums / cross-file join keys.
PRESERVE_KEYS = {
    "sourceDefinitionName",
    "destinationDefinitionName",
    "accountDefinitionName",
    "authenticationType",
    "category",
    "cloudProvider",
    "connectionMode",
    "consent",
    "oneTrustCookieCategory",
    "eventFilteringOption",
    "event_type",
    "key",  # credential key, referenced from configs
    "language",
    "mode",
    "origin",
    "provider",
    "purpose",
    "role",
    "rudderCategory",
    "syncFrequency",
    "type",
    "unit",
    "webhookMethod",
}

# Path fragments under which values are replay/error filters, not customer data.
PRESERVE_PATHS = ("errorConditions",)

# Path segment -> label used when renaming an entity. Matched against the
# innermost segment first: v1 nests destinations under sources, v2 does not, so
# only the nearest segment gives both files the same label for the same entity.
CATEGORIES = {
    "whtProjects": "Project",
    "sources": "Source",
    "destinations": "Destination",
    "accounts": "Account",
    "transformations": "Transformation",
    "destinationTransformations": "Transformation",
    "libraries": "Library",
    "eventReplays": "Replay",
    "credentials": "Credential",
}

NAME_KEYS = {"name", "displayName", "description"}

# Populated from the v2 catalogues: ids that must survive untouched.
definition_ids = set()

# Populated from the v2 catalogues: every name a definition declares. The catalogues are kept
# verbatim, so a config key that appears in one has to survive too, or the join between a
# destination's config and the keys its definition declares (destConfig) breaks. Real key names run
# to 27 base62 characters - allowUserSuppliedJavascript, enableNestedArrayOperations - which is
# exactly the shape of a ksuid.
definition_names = set()

# The per source type keys of a destination config. v2 nests the value under the source type
# (connectionMode: {cloud: "cloud"}), v1 delivers it flattened (connectionMode: "cloud"), so the
# leaf key differs between the two captures and only v1's is caught by PRESERVE_KEYS.
SOURCE_TYPES = {
    "cloud", "cloudSource", "warehouse", "web", "device", "androidKotlin", "iosSwift",
    "android", "ios", "unity", "reactnative", "amp", "flutter", "cordova", "shopify",
}
report = {"ids": 0, "names": 0, "values": 0}


def digest(tag, value):
    return hashlib.sha256(f"{tag}:{value}".encode()).digest()


def encode(raw, alphabet, n):
    x = int.from_bytes(raw, "big")
    out = []
    for _ in range(n):
        out.append(alphabet[x % len(alphabet)])
        x //= len(alphabet)
        if x == 0:
            x = int.from_bytes(digest("pad", "".join(out)), "big")
    return "".join(out)


def fake_id(value):
    report["ids"] += 1
    raw = digest("id", value)
    if prefixed := PREFIXED_KSUID.match(value):  # keep the type prefix, rewrite the id
        return prefixed.group(1) + encode(raw, B62, len(prefixed.group(2)))
    if UUID.match(value):
        h = encode(raw, HEX, 32)
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"
    if XID.match(value):
        return encode(raw, B32, 20)
    return encode(raw, B62, len(value))


def fake_name(value, category):
    report["names"] += 1
    return f"{category} {encode(digest('name', value), B62, 7)}"


def fake_value(value):
    """Length-preserving replacement for an opaque string value."""
    report["values"] += 1
    raw = digest("value", value)
    if EMAIL.match(value):
        local = encode(raw, B62, max(len(value.split("@")[0]), 4))
        return f"{local}@example.com"
    if URL.match(value):
        scheme = value.split("://", 1)[0]
        filler = encode(raw, B62, max(len(value) - len(scheme) - 22, 1))
        return f"{scheme}://example.com/{filler}"
    return encode(raw, B62, len(value))


def category_for(path):
    for segment in reversed(path.split(".")):
        label = CATEGORIES.get(segment.replace("[]", ""))
        if label:
            return label
    return "Entity"


def is_identifier(value):
    return KSUID.match(value) or XID.match(value) or UUID.match(value) or PREFIXED_KSUID.match(value)


def parent_key(path):
    """The key holding the value at `path`, which ends with the value's own key."""
    segments = [segment.replace("[]", "") for segment in path.split(".") if segment]
    return segments[-2] if len(segments) > 1 else ""


def scrub_string(value, key, path):
    if value == "":
        return value
    # a value nested under a per source type key is the same value v1 delivers flattened
    if key in SOURCE_TYPES and parent_key(path) in PRESERVE_KEYS:
        return value
    if is_identifier(value):
        return value if value in definition_ids else fake_id(value)
    if ISO.match(value) or NUMERIC.match(value) or CLOCK.match(value):
        return value
    if value in ("true", "false", "null"):
        return value
    if any(fragment in path for fragment in PRESERVE_PATHS):
        return value
    if key in PRESERVE_KEYS:
        return value
    if key in NAME_KEYS:
        return fake_name(value, category_for(path))
    return fake_value(value)


def scrub_key(key, path):
    if not isinstance(key, str) or not is_identifier(key):
        return key
    if key in definition_ids or key in definition_names:
        return key
    return fake_id(key)


def scrub(node, key=None, path=""):
    if isinstance(node, dict):
        out = {}
        for k, v in node.items():
            if k in DEFINITION_KEYS:  # product metadata, kept verbatim
                out[k] = v
                continue
            # a dict keyed by ids (connections, sources, ...) carries no leaf key
            child_key = k if not is_identifier(str(k)) else key
            out[scrub_key(k, path)] = scrub(v, child_key, f"{path}.{k}")
        return out
    if isinstance(node, list):
        return [scrub(v, key, f"{path}[]") for v in node]
    if isinstance(node, str):
        return scrub_string(node, key, path)
    return node


def collect_definition_ids(v2):
    for catalogue in ("sourceDefinitions", "destinationDefinitions", "accountDefinitions"):
        for name, definition in v2.get(catalogue, {}).items():
            definition_names.add(name)
            if isinstance(definition, dict) and isinstance(definition.get("id"), str):
                definition_ids.add(definition["id"])
            collect_definition_names(definition)


def collect_definition_names(node):
    """Every string a definition declares: its config keys and the key names it lists."""
    if isinstance(node, dict):
        for key, value in node.items():
            definition_names.add(key)
            collect_definition_names(value)
    elif isinstance(node, list):
        for value in node:
            collect_definition_names(value)
    elif isinstance(node, str):
        definition_names.add(node)


# The raw captures carry live credentials, so the scrub refuses to run anywhere a stray `git add`
# could reach them - the filenames below are gitignored, but only a scratch directory keeps the
# rest of the capture out of a public repository too.
def refuse_inside_repository():
    inside = subprocess.run(["git", "rev-parse", "--is-inside-work-tree"],
                            capture_output=True, text=True)
    if inside.stdout.strip() == "true":
        sys.exit("refusing to run inside a git work tree: capture and scrub in a scratch "
                 "directory, see backend-config/testdata/namespace_capture_fixtures.md")


def main():
    refuse_inside_repository()

    v1 = json.load(open("v1-raw.json"))
    v2 = json.load(open("v2-raw.json"))

    collect_definition_ids(v2)
    print(f"preserving {len(definition_ids)} definition ids and "
          f"{len(definition_names)} declared names", file=sys.stderr)

    out1 = scrub(v1, path="v1")
    out2 = scrub(v2, path="v2")

    for name, doc in (("v1-anon.json", out1), ("v2-anon.json", out2)):
        with open(name, "w") as f:
            json.dump(doc, f, separators=(",", ":"), sort_keys=False)
    print(f"rewrote {report['ids']} ids, {report['names']} names, {report['values']} values", file=sys.stderr)


if __name__ == "__main__":
    main()
