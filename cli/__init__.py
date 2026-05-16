"""Lakehouse Studio CLI (`lks`).

Thin client over the Studio HTTP API. No backend coupling — every command
maps to one or more REST/WS calls on the running server (default
http://127.0.0.1:7878). Safe to ship as a standalone wheel.
"""

__version__ = "0.1.0"
