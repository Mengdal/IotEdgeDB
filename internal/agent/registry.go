// Package agent provides agent registration and lifecycle management for iedb.
// It manages edge ingest agent (iedb-agent) registration, heartbeat tracking,
// and table-to-agent mapping for distributed query planning.
package agent

import (
	"sync"
	"time"
)

// AgentInfo holds registration data for one agent.
type AgentInfo struct {
	ID            string
	URL           string
	LastHeartbeat time.Time
	Online        bool
}

// TableMeta describes the real-time data an agent holds for a table.
type TableMeta struct {
	DB       string `json:"db"`
	Table    string `json:"table"`
	MinTime  int64  `json:"min_time"`
	MaxTime  int64  `json:"max_time"`
	RowCount int    `json:"row_count"`
}

// AgentRegistry manages agent registration and table-to-agent mapping.
type AgentRegistry struct {
	mu          sync.RWMutex
	agents      map[string]*AgentInfo              // agent_id -> info
	tableAgents map[string][]string                // "db.table" -> [agent_id]
	agentTables map[string]map[string]TableMeta    // agent_id -> {"db.table": meta}
	timeout     time.Duration
	stopCh      chan struct{}
}

// NewAgentRegistry creates a new registry with the given heartbeat timeout.
func NewAgentRegistry(timeout time.Duration) *AgentRegistry {
	r := &AgentRegistry{
		agents:      make(map[string]*AgentInfo),
		tableAgents: make(map[string][]string),
		agentTables: make(map[string]map[string]TableMeta),
		timeout:     timeout,
		stopCh:      make(chan struct{}),
	}
	go r.cleanupLoop()
	return r
}

// Register adds or updates an agent registration.
func (r *AgentRegistry) Register(id, url string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.agents[id]; ok {
		existing.URL = url
		existing.LastHeartbeat = time.Now()
		existing.Online = true
		return
	}

	r.agents[id] = &AgentInfo{
		ID:            id,
		URL:           url,
		LastHeartbeat: time.Now(),
		Online:        true,
	}
	r.agentTables[id] = make(map[string]TableMeta)
}

// Heartbeat updates agent liveness and table metadata.
func (r *AgentRegistry) Heartbeat(id string, tables []TableMeta) {
	r.mu.Lock()
	defer r.mu.Unlock()

	agent, ok := r.agents[id]
	if !ok {
		return
	}
	agent.LastHeartbeat = time.Now()
	agent.Online = true

	// Update table mappings
	currentTables := r.agentTables[id]
	tableSet := make(map[string]bool)

	for _, t := range tables {
		key := t.DB + "." + t.Table

		if t.RowCount == 0 {
			// Table cleared — remove from agent
			delete(currentTables, key)
			r.removeTableAgent(key, id)
		} else {
			currentTables[key] = t
			r.addTableAgent(key, id)
		}
		tableSet[key] = true
	}

	// Remove tables no longer reported
	for key := range currentTables {
		if !tableSet[key] {
			delete(currentTables, key)
			r.removeTableAgent(key, id)
		}
	}
}

func (r *AgentRegistry) addTableAgent(key, agentID string) {
	agents := r.tableAgents[key]
	for _, a := range agents {
		if a == agentID {
			return
		}
	}
	r.tableAgents[key] = append(agents, agentID)
}

func (r *AgentRegistry) removeTableAgent(key, agentID string) {
	agents := r.tableAgents[key]
	for i, a := range agents {
		if a == agentID {
			r.tableAgents[key] = append(agents[:i], agents[i+1:]...)
			break
		}
	}
	if len(r.tableAgents[key]) == 0 {
		delete(r.tableAgents, key)
	}
}

// GetAgentsForTable returns online agents that have data for the given table.
func (r *AgentRegistry) GetAgentsForTable(db, table string) []*AgentInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := db + "." + table
	agentIDs := r.tableAgents[key]
	result := make([]*AgentInfo, 0, len(agentIDs))
	for _, id := range agentIDs {
		if a, ok := r.agents[id]; ok && a.Online {
			result = append(result, a)
		}
	}
	return result
}

// GetTableMeta returns metadata for a specific agent's table.
func (r *AgentRegistry) GetTableMeta(agentID, db, table string) (TableMeta, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if tables, ok := r.agentTables[agentID]; ok {
		meta, found := tables[db+"."+table]
		return meta, found
	}
	return TableMeta{}, false
}

func (r *AgentRegistry) cleanupLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.cleanup()
		case <-r.stopCh:
			return
		}
	}
}

func (r *AgentRegistry) cleanup() {
	r.mu.Lock()
	defer r.mu.Unlock()

	cutoff := time.Now().Add(-r.timeout)
	for id, agent := range r.agents {
		if agent.LastHeartbeat.Before(cutoff) {
			agent.Online = false
			// Remove all table associations
			if tables, ok := r.agentTables[id]; ok {
				for key := range tables {
					r.removeTableAgent(key, id)
				}
				delete(r.agentTables, id)
			}
		}
	}
}

// ForceCleanup runs the cleanup check immediately (useful in tests).
func (r *AgentRegistry) ForceCleanup() {
	r.cleanup()
}

// Stop shuts down the cleanup loop.
func (r *AgentRegistry) Stop() {
	close(r.stopCh)
}
