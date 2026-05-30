//go:build plugin

package main

import (
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// MapperImpl implements the Mapper interface for NASA access logs.
type MapperImpl struct{}

// Map parses Common Log Format lines and emits the hour of the request.
func (m *MapperImpl) Map(inputs []plugin.MapInput) ([]plugin.Record, error) {
	var records []plugin.Record

	for _, input := range inputs {
		// Example line: in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /path HTTP/1.0" 200 1839
		// We want the hour: "00"
		
		openBracket := strings.Index(input.Value, "[")
		if openBracket == -1 {
			continue
		}
		
		timePart := input.Value[openBracket+1:]
		colonIdx := strings.Index(timePart, ":")
		if colonIdx == -1 || len(timePart) < colonIdx+3 {
			continue
		}
		
		// The hour is right after the first colon in the timestamp
		hour := timePart[colonIdx+1 : colonIdx+3]
		
		records = append(records, plugin.Record{
			Key:   hour,
			Value: "1",
		})
	}

	return records, nil
}

// Mapper is the exported symbol that the worker loads.
var Mapper interface {
	plugin.Mapper
} = &MapperImpl{}
