"""Serve the gateway: ``python -m gateway_fastapi`` (or via ``uvicorn``).

Host/port are configurable via ``AGENTIC_GATEWAY_HOST`` / ``AGENTIC_GATEWAY_PORT``
(defaults 127.0.0.1:8000); the backend via ``AGENTIC_GATEWAY_BACKEND``.
"""

from __future__ import annotations

import os

import uvicorn

from .app import create_app

# Module-level app so `uvicorn gateway_fastapi.__main__:app` also works.
app = create_app()


def main() -> None:
    host = os.environ.get("AGENTIC_GATEWAY_HOST", "127.0.0.1")
    port = int(os.environ.get("AGENTIC_GATEWAY_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":  # pragma: no cover
    main()
