"""
Unit tests for ulmen/core/_routing.py
Target: 100% coverage of AgentRouter and validate_routing_consistency.
"""
from ulmen.core._routing import AgentRouter, validate_routing_consistency


class TestAgentRouterConstruction:
    def test_empty_router(self):
        r = AgentRouter()
        assert r.registered_routes == []

    def test_repr(self):
        r = AgentRouter()
        assert "AgentRouter" in repr(r)
        assert "no" in repr(r)

    def test_repr_with_default(self):
        r = AgentRouter()
        r.register_default(lambda rec: None)
        assert "yes" in repr(r)


class TestAgentRouterRegister:
    def test_register_returns_self(self):
        r = AgentRouter()
        result = r.register("a", "b", lambda rec: None)
        assert result is r

    def test_register_default_returns_self(self):
        r = AgentRouter()
        result = r.register_default(lambda rec: None)
        assert result is r

    def test_registered_routes_listed(self):
        r = AgentRouter()
        r.register("a", "b", lambda rec: None)
        r.register("c", "d", lambda rec: None)
        assert ("a", "b") in r.registered_routes
        assert ("c", "d") in r.registered_routes

    def test_chaining(self):
        received = []
        r = (AgentRouter()
             .register("a", "b", lambda rec: received.append("ab"))
             .register("c", "d", lambda rec: received.append("cd")))
        assert len(r.registered_routes) == 2


class TestAgentRouterDispatch:
    def test_exact_match(self):
        received = []
        r = AgentRouter()
        r.register("agent_a", "agent_b", lambda rec: received.append(rec))
        rec = {"type": "msg", "from_agent": "agent_a", "to_agent": "agent_b"}
        r.dispatch([rec])
        assert len(received) == 1

    def test_no_match_no_default_returns_none(self):
        r = AgentRouter()
        rec = {"type": "msg", "from_agent": "x", "to_agent": "y"}
        results = r.dispatch([rec])
        assert results == [None]

    def test_default_handler_used_when_no_match(self):
        received = []
        r = AgentRouter()
        r.register_default(lambda rec: received.append(rec))
        rec = {"type": "msg", "from_agent": "x", "to_agent": "y"}
        r.dispatch([rec])
        assert len(received) == 1

    def test_wildcard_from_agent(self):
        received = []
        r = AgentRouter()
        r.register("*", "agent_b", lambda rec: received.append("wild_from"))
        rec = {"type": "msg", "from_agent": "any", "to_agent": "agent_b"}
        r.dispatch([rec])
        assert received == ["wild_from"]

    def test_wildcard_to_agent(self):
        received = []
        r = AgentRouter()
        r.register("agent_a", "*", lambda rec: received.append("wild_to"))
        rec = {"type": "msg", "from_agent": "agent_a", "to_agent": "any"}
        r.dispatch([rec])
        assert received == ["wild_to"]

    def test_wildcard_both(self):
        received = []
        r = AgentRouter()
        r.register("*", "*", lambda rec: received.append("both_wild"))
        rec = {"type": "msg", "from_agent": "x", "to_agent": "y"}
        r.dispatch([rec])
        assert received == ["both_wild"]

    def test_exact_takes_priority_over_wildcard(self):
        received = []
        r = AgentRouter()
        r.register("a", "b", lambda rec: received.append("exact"))
        r.register("*", "b", lambda rec: received.append("wild"))
        rec = {"type": "msg", "from_agent": "a", "to_agent": "b"}
        r.dispatch([rec])
        assert received == ["exact"]

    def test_dispatch_multiple_records(self):
        counts = [0]
        r = AgentRouter()
        r.register_default(lambda rec: counts.__setitem__(0, counts[0] + 1))
        recs = [{"from_agent": "a", "to_agent": "b"} for _ in range(5)]
        r.dispatch(recs)
        assert counts[0] == 5

    def test_dispatch_returns_handler_results(self):
        r = AgentRouter()
        r.register("a", "b", lambda rec: 42)
        results = r.dispatch([{"from_agent": "a", "to_agent": "b"}])
        assert results == [42]

    def test_dispatch_empty(self):
        r = AgentRouter()
        assert r.dispatch([]) == []

    def test_none_from_agent(self):
        received = []
        r = AgentRouter()
        r.register("", "b", lambda rec: received.append("empty_from"))
        rec = {"type": "msg", "from_agent": None, "to_agent": "b"}
        r.dispatch([rec])
        assert len(received) == 1

    def test_none_to_agent(self):
        received = []
        r = AgentRouter()
        r.register_default(lambda rec: received.append("default"))
        rec = {"type": "msg", "from_agent": "a", "to_agent": None}
        r.dispatch([rec])
        assert len(received) == 1

    def test_missing_from_to_fields(self):
        received = []
        r = AgentRouter()
        r.register_default(lambda rec: received.append("default"))
        rec = {"type": "msg"}
        r.dispatch([rec])
        assert len(received) == 1

    def test_dispatch_one(self):
        received = []
        r = AgentRouter()
        r.register("a", "b", lambda rec: received.append(rec))
        rec = {"from_agent": "a", "to_agent": "b"}
        r.dispatch_one(rec)
        assert len(received) == 1

    def test_dispatch_one_no_match(self):
        r = AgentRouter()
        result = r.dispatch_one({"from_agent": "x", "to_agent": "y"})
        assert result is None

    def test_handler_return_value_collected(self):
        r = AgentRouter()
        r.register("a", "b", lambda rec: "result_value")
        results = r.dispatch([{"from_agent": "a", "to_agent": "b"}])
        assert results[0] == "result_value"


class TestValidateRoutingConsistency:
    def test_empty_records(self):
        ok, err = validate_routing_consistency([])
        assert ok is True
        assert err is None

    def test_records_without_routing(self):
        recs = [{"type": "msg", "from_agent": None, "to_agent": None}]
        ok, err = validate_routing_consistency(recs)
        assert ok is True

    def test_valid_routing(self):
        recs = [{"from_agent": "a", "to_agent": "b"}]
        ok, err = validate_routing_consistency(recs, known_agents=["a", "b"])
        assert ok is True

    def test_self_loop_fails(self):
        recs = [{"from_agent": "a", "to_agent": "a"}]
        ok, err = validate_routing_consistency(recs)
        assert ok is False
        assert "self-loop" in err

    def test_unknown_from_agent(self):
        recs = [{"from_agent": "unknown", "to_agent": "b"}]
        ok, err = validate_routing_consistency(recs, known_agents=["b"])
        assert ok is False
        assert "from_agent" in err

    def test_unknown_to_agent(self):
        recs = [{"from_agent": "a", "to_agent": "unknown"}]
        ok, err = validate_routing_consistency(recs, known_agents=["a"])
        assert ok is False
        assert "to_agent" in err

    def test_mixed_presence_fails(self):
        recs = [{"from_agent": "a", "to_agent": None}]
        ok, err = validate_routing_consistency(recs)
        assert ok is False
        assert "both" in err

    def test_mixed_presence_reverse(self):
        recs = [{"from_agent": None, "to_agent": "b"}]
        ok, err = validate_routing_consistency(recs)
        assert ok is False

    def test_no_known_agents_no_whitelist_check(self):
        recs = [{"from_agent": "any_agent", "to_agent": "other_agent"}]
        ok, err = validate_routing_consistency(recs)
        assert ok is True

    def test_multiple_records_all_valid(self):
        recs = [
            {"from_agent": "a", "to_agent": "b"},
            {"from_agent": "b", "to_agent": "c"},
        ]
        ok, err = validate_routing_consistency(recs, known_agents=["a", "b", "c"])
        assert ok is True

    def test_multiple_records_one_bad(self):
        recs = [
            {"from_agent": "a", "to_agent": "b"},
            {"from_agent": "a", "to_agent": "a"},
        ]
        ok, err = validate_routing_consistency(recs)
        assert ok is False

    def test_row_number_in_error(self):
        recs = [
            {"from_agent": "a", "to_agent": "b"},
            {"from_agent": "x", "to_agent": "x"},
        ]
        ok, err = validate_routing_consistency(recs)
        assert "2" in err

    def test_records_with_no_routing_fields(self):
        recs = [{"type": "msg", "id": "m1"}]
        ok, err = validate_routing_consistency(recs)
        assert ok is True

    def test_known_agents_empty_list(self):
        # Empty known_agents list: frozenset([]) means every agent fails
        # But actual behavior: empty list converts to empty frozenset,
        # so "a" not in frozenset([]) is True -> should fail
        # However implementation uses: known = frozenset(known_agents) if known_agents else None
        # An empty list is falsy, so known=None, no whitelist check applied
        recs = [{"from_agent": "a", "to_agent": "b"}]
        ok, err = validate_routing_consistency(recs, known_agents=[])
        # Empty list is falsy -> no whitelist check -> passes
        assert ok is True

    def test_known_agents_non_matching(self):
        recs = [{"from_agent": "a", "to_agent": "b"}]
        ok, err = validate_routing_consistency(recs, known_agents=["x", "y"])
        assert ok is False
        assert "from_agent" in err
