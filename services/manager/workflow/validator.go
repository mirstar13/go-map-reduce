package workflow

import (
	"fmt"
)

// Dependency represents a directed edge from Parent to Child.
type Dependency struct {
	Parent string
	Child  string
}

// ValidateDAG ensures the given stages and dependencies form a Directed Acyclic Graph.
func ValidateDAG(stages []string, dependencies []Dependency) error {
	adj := make(map[string][]string)
	for _, s := range stages {
		adj[s] = []string{}
	}

	for _, d := range dependencies {
		if _, ok := adj[d.Parent]; !ok {
			return fmt.Errorf("dependency refers to unknown parent stage: %s", d.Parent)
		}
		if _, ok := adj[d.Child]; !ok {
			return fmt.Errorf("dependency refers to unknown child stage: %s", d.Child)
		}
		adj[d.Parent] = append(adj[d.Parent], d.Child)
	}

	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	for _, s := range stages {
		if !visited[s] {
			if hasCycle(s, adj, visited, recStack) {
				return fmt.Errorf("workflow contains a cycle")
			}
		}
	}

	return nil
}

func hasCycle(v string, adj map[string][]string, visited, recStack map[string]bool) bool {
	visited[v] = true
	recStack[v] = true

	for _, neighbor := range adj[v] {
		if !visited[neighbor] {
			if hasCycle(neighbor, adj, visited, recStack) {
				return true
			}
		} else if recStack[neighbor] {
			return true
		}
	}

	recStack[v] = false
	return false
}
