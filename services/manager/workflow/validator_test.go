package workflow

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestValidateDAG(t *testing.T) {
	tests := []struct {
		name         string
		stages       []string
		dependencies []Dependency
		wantErr      bool
	}{
		{
			name:   "simple linear DAG",
			stages: []string{"A", "B"},
			dependencies: []Dependency{
				{Parent: "A", Child: "B"},
			},
			wantErr: false,
		},
		{
			name:   "diamond DAG",
			stages: []string{"A", "B", "C", "D"},
			dependencies: []Dependency{
				{Parent: "A", Child: "B"},
				{Parent: "A", Child: "C"},
				{Parent: "B", Child: "D"},
				{Parent: "C", Child: "D"},
			},
			wantErr: false,
		},
		{
			name:   "cycle detection",
			stages: []string{"A", "B"},
			dependencies: []Dependency{
				{Parent: "A", Child: "B"},
				{Parent: "B", Child: "A"},
			},
			wantErr: true,
		},
		{
			name:   "unknown parent",
			stages: []string{"A"},
			dependencies: []Dependency{
				{Parent: "X", Child: "A"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDAG(tt.stages, tt.dependencies)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
