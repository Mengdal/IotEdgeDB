package agent

import (
	"testing"
	"time"
)

// TestHeartbeatAfterOfflineCleanup is a regression test: a reconnect heartbeat
// after cleanup previously panicked with "assignment to entry in nil map".
//
// Sequence: register + heartbeat, let the heartbeat time out, run cleanup
// (which deletes the agent's table map but keeps the agents entry), then
// heartbeat again with tables. The reconnecting heartbeat must not panic and
// must repopulate the table mapping.
func TestHeartbeatAfterOfflineCleanup(t *testing.T) {
	r := NewAgentRegistry(100 * time.Millisecond)
	defer r.Stop()

	r.Register("edge-01", "http://edge-01:8080")
	r.Heartbeat("edge-01", "http://edge-01:8080", []TableMeta{
		{DB: "db1", Table: "cpu", MinTime: 100, MaxTime: 200, RowCount: 10},
	})

	// Let the heartbeat expire, then force cleanup (the background loop runs
	// every 10s, far slower than the test). This deletes agentTables["edge-01"].
	time.Sleep(150 * time.Millisecond)
	r.ForceCleanup()

	if agents := r.GetAgentsForTable("db1", "cpu"); len(agents) != 0 {
		t.Fatalf("expected table mapping cleared after cleanup, got %d agent(s)", len(agents))
	}

	// Reconnect: must not panic, and must restore the table with fresh metadata.
	r.Heartbeat("edge-01", "http://edge-01:8080", []TableMeta{
		{DB: "db1", Table: "cpu", MinTime: 200, MaxTime: 300, RowCount: 20},
	})

	agents := r.GetAgentsForTable("db1", "cpu")
	if len(agents) != 1 || agents[0].ID != "edge-01" {
		t.Fatalf("expected edge-01 back online with its table, got %d agent(s)", len(agents))
	}

	meta, ok := r.GetTableMeta("edge-01", "db1", "cpu")
	if !ok {
		t.Fatal("expected table metadata restored after reconnecting heartbeat")
	}
	if meta.RowCount != 20 {
		t.Fatalf("expected refreshed row count 20, got %d", meta.RowCount)
	}
	if !agents[0].Online {
		t.Fatal("expected the reconnecting agent to be online")
	}
}

// TestHeartbeatEmptyTables clears a previously reported table when the agent
// reports an empty list, and records no rows otherwise.
func TestHeartbeatEmptyTables(t *testing.T) {
	r := NewAgentRegistry(time.Minute)
	defer r.Stop()

	r.Register("edge-01", "http://edge-01:8080")
	r.Heartbeat("edge-01", "http://edge-01:8080", []TableMeta{
		{DB: "db1", Table: "cpu", MinTime: 100, MaxTime: 200, RowCount: 10},
	})
	if agents := r.GetAgentsForTable("db1", "cpu"); len(agents) != 1 {
		t.Fatalf("expected table mapped after heartbeat, got %d agent(s)", len(agents))
	}

	// A follow-up heartbeat that no longer reports the table must clear it.
	r.Heartbeat("edge-01", "http://edge-01:8080", nil)
	if agents := r.GetAgentsForTable("db1", "cpu"); len(agents) != 0 {
		t.Fatalf("expected table cleared by empty heartbeat, got %d agent(s)", len(agents))
	}
}

// TestListSortsByID checks the monitoring snapshot ordering.
func TestListSortsByID(t *testing.T) {
	r := NewAgentRegistry(time.Minute)
	defer r.Stop()

	r.Register("edge-02", "http://edge-02:8080")
	r.Register("edge-01", "http://edge-01:8080")

	list := r.List()
	if len(list) != 2 {
		t.Fatalf("expected 2 agents, got %d", len(list))
	}
	if list[0].ID != "edge-01" || list[1].ID != "edge-02" {
		t.Fatalf("expected IDs sorted, got %q then %q", list[0].ID, list[1].ID)
	}
	if !list[0].Online || !list[1].Online {
		t.Fatal("expected freshly registered agents to be online")
	}
}

// TestHeartbeatAutoRegisters simulates a hub restart: the in-memory registry
// is empty while an agent that never restarts keeps heartbeating. A heartbeat
// carrying a url must re-create the agent (with its tables) so it recovers
// without a re-register.
func TestHeartbeatAutoRegisters(t *testing.T) {
	r := NewAgentRegistry(time.Minute)
	defer r.Stop()

	// No Register call — as though the hub restarted and forgot this agent.
	r.Heartbeat("edge-01", "http://edge-01:8080", []TableMeta{
		{DB: "db1", Table: "cpu", MinTime: 100, MaxTime: 200, RowCount: 10},
	})

	agents := r.GetAgentsForTable("db1", "cpu")
	if len(agents) != 1 || agents[0].ID != "edge-01" {
		t.Fatalf("expected the heartbeating agent auto-registered, got %d agent(s)", len(agents))
	}
	if !agents[0].Online {
		t.Fatal("expected the auto-registered agent online")
	}
	if agents[0].URL != "http://edge-01:8080" {
		t.Fatalf("expected the heartbeat url carried over, got %q", agents[0].URL)
	}

	meta, ok := r.GetTableMeta("edge-01", "db1", "cpu")
	if !ok || meta.RowCount != 10 {
		t.Fatalf("expected table metadata restored by auto-registration, got %+v, ok=%v", meta, ok)
	}
}

// TestHeartbeatWithoutURLDoesNotRegister keeps the old behaviour: a heartbeat
// for an unknown id without a url is ignored, because an entry with no url
// could never be reached by query merging.
func TestHeartbeatWithoutURLDoesNotRegister(t *testing.T) {
	r := NewAgentRegistry(time.Minute)
	defer r.Stop()

	r.Heartbeat("ghost", "", []TableMeta{
		{DB: "db1", Table: "cpu", MinTime: 1, MaxTime: 2, RowCount: 1},
	})

	if list := r.List(); len(list) != 0 {
		t.Fatalf("expected no agent registered from a url-less heartbeat, got %d", len(list))
	}
}
