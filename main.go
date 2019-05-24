package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/direct"
	"os"
	"strings"
)

type State struct {
	State string // 0
	Group string // 1
	Total string // 4
}

func readStateData() (*[]State, error) {
	file, openErr := os.Open("internal/data/State.csv")

	if openErr != nil {
		return nil, openErr
	}

	lines, readErr := csv.NewReader(file).ReadAll()

	if readErr != nil {
		return nil, readErr
	}

	var states []State
	for _, line := range lines {
		state := State{
			State: line[0],
			Group: line[1],
			Total: line[4],
		}

		states = append(states, state)
	}

	return &states, nil
}

func main() {
	states, dataErr := readStateData()

	if dataErr != nil {
		panic(dataErr)
	}

	pipeline, scope := beam.NewPipelineWithRoot()

	local.New(context.Background())

	lines := beam.CreateList(scope, *states)

	keyed := beam.ParDo(scope, func(state State) (string, State) {
		return state.State, state
	}, lines)

	grouped := beam.GroupByKey(scope, keyed)

	formatted := beam.ParDo(scope, func(key string, values func(*State) bool) string {
		var state State
		var retVal strings.Builder
		retVal.WriteString(key)
		retVal.WriteString("\n-------------\n")
		for values(&state) {
			retVal.WriteString(fmt.Sprintf("%s: %s", state.Group, state.Total))
			retVal.WriteString("\n")
		}

		return retVal.String()
	}, grouped)

	textio.Write(scope, "states.txt", formatted)

	if err := direct.Execute(context.Background(), pipeline); err != nil {
		panic(err)
	}
}
