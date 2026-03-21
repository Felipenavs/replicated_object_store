import argparse


MAX_VALUE_SIZE = 1_048_576  # 1 MiB (maximum allowed size for stored values)

def is_valid_key(s: str) -> bool:
    """Check that every character in s is a printable, non-space ASCII character (0x21-0x7E)."""
    return all(0x21 <= ord(c) <= 0x7E for c in s)

def validate_key(key: str) -> bool:
    """
    Return True if key is a non-empty string of 1-128 printable ASCII characters.
    Rejects non-strings, empty strings, strings over 128 chars, and any character
    outside the printable ASCII range.
    """
    return isinstance(key, str) and (len(key) > 0 and len(key) <= 128) and is_valid_key(key)

def validate_value(value: bytes) -> bool:
    """
    Return True if value is a non-empty bytes object within the 1 MiB size limit.
    Rejects non-bytes types, empty payloads, and anything exceeding MAX_VALUE_SIZE.
    """
    return isinstance(value, bytes) and len(value) > 0 and len(value) <= MAX_VALUE_SIZE

def is_valid_server(s: str) -> bool:
    """
    Return True if s looks like a valid 'host:port' address.
    Requires a colon separator, a non-empty host, and a numeric port.
    Does not perform DNS resolution or range-check the port number.
    """
    if ":" not in s:
        return False
    host, port = s.split(":", 1)
    return host and port.isdigit()

def next_read_stub(q):
    """
    Advance a deque-based round-robin cursor and return the current front element.
    If the queue has more than one entry, rotate left so the next call returns the
    following element. Always returns the element now at index 0.
    """
    if len(q) > 1:
        q.rotate(-1)  # shift all elements left by one position
    return q[0]

def parse_value(value):

    """
    - Joins all tokens with spaces and strips surrounding whitespace.
    - If the result is wrapped in matching single or double quotes, strips them
      and returns the inner content (preserving interior whitespace).
    - Otherwise, returns the first non-empty token after stripping individual
      tokens, or None if every token is blank.
    """
    s = " ".join(value)
    s = s.strip()

    # remove the outer quote pair and return the inner string as-is
    if (s.startswith('"') and s.endswith('"')) or \
       (s.startswith("'") and s.endswith("'")):
        return s[1:-1]

    #return the first token that is non-empty after stripping
    return next((c for item in value if (c := item.strip())), None)

def get_servers(nodes) -> list[str]:
    """
    Validate, normalise, and sort a list of raw server address strings.
    Each entry in nodes is stripped of whitespace and lower-cased, then checked
    with is_valid_server(). Returns a sorted list of the cleaned addresses if
    every node is valid, or None if any entry fails validation.
    """
    servers = sorted(
        s 
        for server in nodes 
        if (s := server.strip().lower()) and is_valid_server(s)  # normalise then validate
    )

    # If any node was invalid it would have been filtered out, making lengths differ
    if len(servers) < len(nodes):
        return None
    return servers

def parse_args() -> argparse.Namespace:
    """Parse CLI args.

    Required flags:
    - --listen 
    - --cluster
    """
    p = argparse.ArgumentParser(description="Object store server")
    p.add_argument("--listen", required=True, help="Address to listen on in the format <host>:<port>")
    p.add_argument("--cluster", required=True, nargs="+", help="List of cluster nodes in the format <host>:<port>")
    return p.parse_args()








