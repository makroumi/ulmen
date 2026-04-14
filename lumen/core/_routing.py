"""
LUMEN-AGENT routing layer.

AgentRouter dispatches decoded records to registered handler callables
based on the (from_agent, to_agent) pair declared in meta fields.

Routing consistency validation verifies that all from_agent/to_agent
values in a payload form a consistent directed graph with no
unregistered endpoints.
"""

from __future__ import annotations

from typing import Any, Callable


class AgentRouter:
    """
    Dispatch agent records to handlers by (from_agent, to_agent) pair.

    Registration
    ------------
    router = AgentRouter()
    router.register("agent_a", "agent_b", handler_fn)
    router.register_default(fallback_fn)   # optional catch-all

    Dispatch
    --------
    results = router.dispatch(records)

    Each handler receives a single record dict and may return any value.
    Results are collected into a list in record order.

    Routing key lookup order
    ------------------------
    1. Exact (from_agent, to_agent) match
    2. Wildcard (from_agent, "*") match
    3. Wildcard ("*", to_agent) match
    4. Default handler ("*", "*")
    5. None — record is skipped, None appended to results
    """

    def __init__(self) -> None:
        self._routes: dict[tuple[str, str], Callable] = {}
        self._default: Callable | None = None

    def register(
        self,
        from_agent: str,
        to_agent: str,
        handler: Callable[[dict], Any],
    ) -> AgentRouter:
        """Register handler for (from_agent, to_agent) pair. Returns self for chaining."""
        self._routes[(from_agent, to_agent)] = handler
        return self

    def register_default(self, handler: Callable[[dict], Any]) -> AgentRouter:
        """Register a catch-all handler for unmatched routes. Returns self."""
        self._default = handler
        return self

    def _resolve(self, from_agent: str | None, to_agent: str | None) -> Callable | None:
        fa = from_agent or ""
        ta = to_agent   or ""
        return (
            self._routes.get((fa, ta))
            or self._routes.get((fa, "*"))
            or self._routes.get(("*", ta))
            or self._routes.get(("*", "*"))
            or self._default
        )

    def dispatch(self, records: list[dict]) -> list[Any]:
        """
        Dispatch each record to its registered handler.

        Returns list of handler return values (None for unrouted records).
        """
        results = []
        for rec in records:
            fa      = rec.get("from_agent")
            ta      = rec.get("to_agent")
            handler = self._resolve(fa, ta)
            results.append(handler(rec) if handler is not None else None)
        return results

    def dispatch_one(self, rec: dict) -> Any:
        """Dispatch a single record. Returns handler result or None."""
        handler = self._resolve(rec.get("from_agent"), rec.get("to_agent"))
        return handler(rec) if handler is not None else None

    @property
    def registered_routes(self) -> list[tuple[str, str]]:
        """List of all explicitly registered (from_agent, to_agent) pairs."""
        return list(self._routes.keys())

    def __repr__(self) -> str:
        return f"AgentRouter(routes={len(self._routes)}, default={'yes' if self._default else 'no'})"


# ---------------------------------------------------------------------------
# Routing consistency validation
# ---------------------------------------------------------------------------

def validate_routing_consistency(
    records: list[dict],
    known_agents: list[str] | None = None,
) -> tuple[bool, str | None]:
    """
    Validate that from_agent / to_agent values in records form a consistent
    routing graph.

    Checks performed
    ----------------
    1. from_agent and to_agent are both present or both absent per record.
    2. No record has from_agent == to_agent (self-loop).
    3. If known_agents is provided, all from/to values must be in that set.

    Parameters
    ----------
    records      : list of decoded agent record dicts
    known_agents : optional whitelist of valid agent identifiers

    Returns
    -------
    (True, None)        — consistent
    (False, str)        — inconsistent, reason string
    """
    known = frozenset(known_agents) if known_agents else None

    for i, rec in enumerate(records):
        fa = rec.get("from_agent")
        ta = rec.get("to_agent")
        row = i + 1

        # Both present or both absent
        if (fa is None) != (ta is None):
            return False, (
                f"Row {row}: from_agent and to_agent must both be set or both absent "
                f"(from_agent={fa!r}, to_agent={ta!r})"
            )

        if fa is None:
            continue

        # Self-loop check
        if fa == ta:
            return False, (
                f"Row {row}: from_agent == to_agent == {fa!r} (self-loop not allowed)"
            )

        # Whitelist check
        if known is not None:
            if fa not in known:
                return False, (
                    f"Row {row}: from_agent {fa!r} not in known_agents {sorted(known)}"
                )
            if ta not in known:
                return False, (
                    f"Row {row}: to_agent {ta!r} not in known_agents {sorted(known)}"
                )

    return True, None
