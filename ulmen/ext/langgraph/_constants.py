# Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
# ULMEN is licensed under the Business Source License 1.1.
"""
Extension-level constants for ulmen-langgraph.

All tuneable defaults live here so they are changed in one place.
Nothing in this file imports from langgraph or langchain — it is
safe to import at module load time before optional deps are checked.
"""

# Package identity
EXT_NAME    = "ulmen-langgraph"
EXT_VERSION = "0.1.0"

# Minimum supported versions (checked at runtime in _compat.py)
MIN_LANGGRAPH_VERSION     = (1, 0, 0)
MIN_LANGCHAIN_CORE_VERSION = (0, 1, 0)

# Serializer defaults
DEFAULT_ZLIB_LEVEL   = 6   # 0-9, 6 is zlib default, good balance
DEFAULT_POOL_LIMIT   = 128 # string interning pool size for checkpoint records

# Reducer defaults
DEFAULT_COMPRESS_STRATEGY = "completed_sequences"
DEFAULT_CONTEXT_WINDOW    = 8000  # tokens, used when graph does not declare one

# Stream defaults
DEFAULT_CHUNK_SIZE = 65_536  # bytes per streamed binary chunk

# Store defaults
STORE_ULMEN_MARKER = b"ULMX"  # 4-byte prefix on every store value
                               # lets UlmenStore detect its own values on read
                               # and pass foreign values through unchanged

# Handoff defaults
HANDOFF_CONFIDENCE = 1.0   # mem record confidence for state handoff records
HANDOFF_TTL        = -1    # permanent

# Key used in Checkpoint.channel_values to store ULMEN metadata
ULMEN_META_CHANNEL = "__ulmen_meta__"
