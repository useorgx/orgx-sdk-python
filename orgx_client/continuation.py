"""Portable reconstruction of exact server bytes; never an authorization token."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Mapping, Optional

MAX_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class ContextContinuation:
    version: str
    serialized: str
    data: Mapping[str, Any]


def _version(text: str) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) > MAX_BYTES:
        raise ValueError("Context transfer exceeds the client bound")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def apply_context_transfer(response: Mapping[str, Any], base: Optional[ContextContinuation] = None) -> ContextContinuation:
    transfer = response.get("context_transfer")
    if not isinstance(transfer, dict) or transfer.get("schema_version") != "orgx.context-transfer/v1":
        raise ValueError("Unsupported context transfer")
    version = transfer.get("version")
    if not isinstance(version, str) or not re.fullmatch(r"sha256:[a-f0-9]{64}", version):
        raise ValueError("Invalid context transfer version")
    if transfer.get("mode") == "full" and isinstance(transfer.get("serialized"), str):
        serialized = transfer["serialized"]
    elif transfer.get("mode") == "delta":
        if base is None or transfer.get("base_version") != base.version or _version(base.serialized) != base.version:
            raise ValueError("Context base mismatch; rebootstrap required")
        operations = transfer.get("operations")
        if not isinstance(operations, list) or len(operations) > 65536:
            raise ValueError("Invalid context delta")
        # Match JS newline semantics exactly, including literal Unicode separators.
        lines = re.findall(r"[^\n]*\n|[^\n]+$", base.serialized)
        chunks = []
        size = 0
        for operation in operations:
            if not isinstance(operation, dict) or len(operation) != 1:
                raise ValueError("Invalid context delta operation")
            if isinstance(operation.get("insert"), str):
                chunk = operation["insert"]
            else:
                span = operation.get("copy")
                if not isinstance(span, list) or len(span) != 2 or any(type(n) is not int for n in span):
                    raise ValueError("Invalid context delta range")
                start, count = span
                if start < 0 or count <= 0 or start + count > len(lines):
                    raise ValueError("Invalid context delta range")
                chunk = "".join(lines[start:start + count])
            size += len(chunk.encode("utf-8"))
            if size > MAX_BYTES:
                raise ValueError("Context transfer exceeds the client bound")
            chunks.append(chunk)
        serialized = "".join(chunks)
    else:
        raise ValueError("Unsupported context transfer mode")
    if _version(serialized) != version:
        raise ValueError("Context transfer digest mismatch")
    data = json.loads(serialized)
    if not isinstance(data, dict) or not isinstance(data.get("context_delivery"), dict):
        raise ValueError("Context delivery metadata is missing")
    return ContextContinuation(version=version, serialized=serialized, data=data)
