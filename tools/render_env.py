#!/usr/bin/env python3
"""Fill `__NAME__` placeholders in an .env template from environment variables.

Used by setup.sh. It replaces a chain of `sed "s|__X__|$VALUE|g"` calls, which
corrupted any value containing `&`, `|` or `\\` (sed reads `&` in a replacement
as "the matched text"). Values are written literally, except that `$` is
doubled: docker compose interpolates `$` in .env files.

    python3 tools/render_env.py TEMPLATE OUTPUT   # reads values from the environment
"""
import os
import re
import sys

_PLACEHOLDER = re.compile(r"__([A-Z][A-Z0-9_]*)__")


def render(template: str, values: dict) -> str:
    """Substitute every placeholder; a placeholder with no value raises KeyError."""
    def fill(match):
        name = match.group(1)
        if name not in values:
            raise KeyError(f"no value for placeholder __{name}__")
        return values[name].replace("$", "$$")
    return _PLACEHOLDER.sub(fill, template)


def main(argv: list) -> int:
    template_path, output_path = argv
    with open(template_path) as f:
        text = render(f.read(), dict(os.environ))
    fd = os.open(output_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
