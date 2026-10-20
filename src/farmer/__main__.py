"""``python -m farmer`` -> the ``farmer`` CLI."""

import sys

from farmer.cli import main

if __name__ == "__main__":
    sys.exit(main())
