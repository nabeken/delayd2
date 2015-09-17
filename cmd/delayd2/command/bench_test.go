package command

import (
	"testing"

	"github.com/mitchellh/cli"
	"github.com/stretchr/testify/assert"
)

func TestBenchCommand_implement(t *testing.T) {
	var _ cli.Command = &BenchCommand{}
}

type TestCase struct {
	N        int
	Expected [][]string
}

func TestDistributeN(t *testing.T) {
	assert := assert.New(t)

	testCases := []struct {
		Payloads []string
		TestCase []TestCase
	}{
		{
			Payloads: []string{"1"},
			TestCase: []TestCase{
				{
					N: 1,
					Expected: [][]string{
						[]string{"1"},
					},
				},
				{
					N: 2,
					Expected: [][]string{
						[]string{"1"},
					},
				},
			},
		},
		{
			Payloads: []string{"1", "2", "3"},
			TestCase: []TestCase{
				{
					N: 1,
					Expected: [][]string{
						[]string{"1", "2", "3"},
					},
				},
				{
					N: 2,
					Expected: [][]string{
						[]string{"1", "3"},
						[]string{"2"},
					},
				},
				{
					N: 3,
					Expected: [][]string{
						[]string{"1"},
						[]string{"2"},
						[]string{"3"},
					},
				},
				{
					N: 4,
					Expected: [][]string{
						[]string{"1"},
						[]string{"2"},
						[]string{"3"},
					},
				},
			},
		},
		{
			Payloads: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
			TestCase: []TestCase{
				{
					N: 6,
					Expected: [][]string{
						[]string{"1", "7"},
						[]string{"2", "8"},
						[]string{"3", "9"},
						[]string{"4", "10"},
						[]string{"5"},
						[]string{"6"},
					},
				},
			},
		},
	}
	for i := range testCases {
		for _, tc := range testCases[i].TestCase {
			actual := DistributeN(tc.N, testCases[i].Payloads)
			t.Log(actual)
			assert.Len(actual, len(tc.Expected))
			assert.Equal(tc.Expected, actual)
		}
	}
}
