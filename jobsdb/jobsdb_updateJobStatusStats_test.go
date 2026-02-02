package jobsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateJobStatusStats(t *testing.T) {
	t.Run("Merge", func(t *testing.T) {
		t.Run("empty", func(t *testing.T) {
			stats1 := updateJobStatusStats{}
			stats2 := updateJobStatusStats{}

			stats1.Merge(stats2)

			assert.Empty(t, stats1)
		})

		t.Run("merge into empty", func(t *testing.T) {
			stats1 := updateJobStatusStats{}
			stats2 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
							},
						},
					},
				},
			}

			stats1.Merge(stats2)

			require.Contains(t, stats1, partitionIDKey("partition1"))
			require.Contains(t, stats1[partitionIDKey("partition1")], workspaceIDKey("workspace1"))
			require.Contains(t, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")], customValKey("customVal1"))
			require.Contains(t, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")], jobStateKey("failed"))
			require.Contains(t, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")], parameterFiltersKey("param1:value1"))

			mergedStats := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 5, mergedStats.count)
			assert.Equal(t, 100, mergedStats.bytes)
			assert.Equal(t, "param1:value1", mergedStats.parameters.String())
		})

		t.Run("merge from empty", func(t *testing.T) {
			stats1 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
							},
						},
					},
				},
			}
			stats2 := updateJobStatusStats{}

			stats1.Merge(stats2)

			require.Contains(t, stats1, partitionIDKey("partition1"))
			require.Contains(t, stats1[partitionIDKey("partition1")], workspaceIDKey("workspace1"))
			require.Contains(t, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")], customValKey("customVal1"))
			require.Contains(t, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")], jobStateKey("failed"))
			require.Contains(t, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")], parameterFiltersKey("param1:value1"))

			mergedStats := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 5, mergedStats.count)
			assert.Equal(t, 100, mergedStats.bytes)
		})

		t.Run("merge same workspace and state", func(t *testing.T) {
			stats1 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
							},
						},
					},
				},
			}
			stats2 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      3,
									bytes:      50,
								},
							},
						},
					},
				},
			}

			stats1.Merge(stats2)

			mergedStats := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 8, mergedStats.count)   // 5 + 3
			assert.Equal(t, 150, mergedStats.bytes) // 100 + 50
		})

		t.Run("merge different parameters", func(t *testing.T) {
			stats1 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
							},
						},
					},
				},
			}
			stats2 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param2:value2"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param2", Value: "value2"}},
									count:      3,
									bytes:      50,
								},
							},
						},
					},
				},
			}

			stats1.Merge(stats2)

			assert.Contains(t, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")], parameterFiltersKey("param1:value1"))
			assert.Contains(t, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")], parameterFiltersKey("param2:value2"))

			stats1Merged := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 5, stats1Merged.count)
			assert.Equal(t, 100, stats1Merged.bytes)

			stats2Merged := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param2:value2")]
			assert.Equal(t, 3, stats2Merged.count)
			assert.Equal(t, 50, stats2Merged.bytes)
		})

		t.Run("merge different states", func(t *testing.T) {
			stats1 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
							},
						},
					},
				},
			}
			stats2 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("succeeded"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      3,
									bytes:      50,
								},
							},
						},
					},
				},
			}

			stats1.Merge(stats2)

			assert.Contains(t, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")], jobStateKey("failed"))
			assert.Contains(t, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")], jobStateKey("succeeded"))

			failedStats := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 5, failedStats.count)
			assert.Equal(t, 100, failedStats.bytes)

			succeededStats := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("succeeded")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 3, succeededStats.count)
			assert.Equal(t, 50, succeededStats.bytes)
		})

		t.Run("merge different workspaces", func(t *testing.T) {
			stats1 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
							},
						},
					},
				},
			}
			stats2 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace2"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      3,
									bytes:      50,
								},
							},
						},
					},
				},
			}

			stats1.Merge(stats2)

			assert.Contains(t, stats1, partitionIDKey("partition1"))

			assert.Contains(t, stats1[partitionIDKey("partition1")], workspaceIDKey("workspace1"))
			assert.Contains(t, stats1[partitionIDKey("partition1")], workspaceIDKey("workspace2"))

			ws1Stats := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 5, ws1Stats.count)
			assert.Equal(t, 100, ws1Stats.bytes)

			ws2Stats := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace2")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 3, ws2Stats.count)
			assert.Equal(t, 50, ws2Stats.bytes)
		})

		t.Run("merge different partitions", func(t *testing.T) {
			stats1 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
							},
						},
					},
				},
			}
			stats2 := updateJobStatusStats{
				partitionIDKey("partition2"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      3,
									bytes:      50,
								},
							},
						},
					},
				},
			}

			stats1.Merge(stats2)

			assert.Contains(t, stats1, partitionIDKey("partition1"))
			assert.Contains(t, stats1, partitionIDKey("partition2"))

			ws1Stats := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 5, ws1Stats.count)
			assert.Equal(t, 100, ws1Stats.bytes)

			ws2Stats := stats1[partitionIDKey("partition2")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 3, ws2Stats.count)
			assert.Equal(t, 50, ws2Stats.bytes)
		})

		t.Run("merge different custom values", func(t *testing.T) {
			stats1 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
							},
						},
					},
				},
			}
			stats2 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal2"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      3,
									bytes:      50,
								},
							},
						},
					},
				},
			}

			stats1.Merge(stats2)

			assert.Contains(t, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")], customValKey("customVal1"))
			assert.Contains(t, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")], customValKey("customVal2"))

			cv1Stats := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 5, cv1Stats.count)
			assert.Equal(t, 100, cv1Stats.bytes)

			cv2Stats := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal2")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 3, cv2Stats.count)
			assert.Equal(t, 50, cv2Stats.bytes)
		})

		t.Run("merge same custom value same state", func(t *testing.T) {
			stats1 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
							},
						},
					},
				},
			}
			stats2 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace2"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      3,
									bytes:      50,
								},
							},
						},
					},
				},
			}

			stats1.Merge(stats2)

			assert.Contains(t, stats1[partitionIDKey("partition1")], workspaceIDKey("workspace1"))
			assert.Contains(t, stats1[partitionIDKey("partition1")], workspaceIDKey("workspace2"))

			ws1Stats := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 5, ws1Stats.count)
			assert.Equal(t, 100, ws1Stats.bytes)

			ws2Stats := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace2")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 3, ws2Stats.count)
			assert.Equal(t, 50, ws2Stats.bytes)
		})

		t.Run("merge different custom values", func(t *testing.T) {
			stats1 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
							},
						},
					},
				},
			}
			stats2 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal2"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      3,
									bytes:      50,
								},
							},
						},
					},
				},
			}

			stats1.Merge(stats2)

			assert.Contains(t, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")], customValKey("customVal1"))
			assert.Contains(t, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")], customValKey("customVal2"))

			cv1Stats := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 5, cv1Stats.count)
			assert.Equal(t, 100, cv1Stats.bytes)

			cv2Stats := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal2")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 3, cv2Stats.count)
			assert.Equal(t, 50, cv2Stats.bytes)
		})

		t.Run("merge same custom value different states", func(t *testing.T) {
			stats1 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
							},
						},
					},
					workspaceIDKey("workspace2"): {
						customValKey("customVal1"): {
							jobStateKey("succeeded"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      3,
									bytes:      50,
								},
							},
						},
					},
				},
			}
			stats2 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace3"): {
						customValKey("customVal1"): {
							jobStateKey("aborted"): {
								parameterFiltersKey("param2:value2"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param2", Value: "value2"}},
									count:      2,
									bytes:      20,
								},
							},
						},
					},
				},
			}

			stats1.Merge(stats2)

			assert.Contains(t, stats1[partitionIDKey("partition1")], workspaceIDKey("workspace1"))
			assert.Contains(t, stats1[partitionIDKey("partition1")], workspaceIDKey("workspace2"))
			assert.Contains(t, stats1[partitionIDKey("partition1")], workspaceIDKey("workspace3"))

			// Check that all the custom values and states are preserved
			assert.Contains(t, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")], jobStateKey("failed"))
			assert.Contains(t, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace2")][customValKey("customVal1")], jobStateKey("succeeded"))
			assert.Contains(t, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace3")][customValKey("customVal1")], jobStateKey("aborted"))

			failedStats := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 5, failedStats.count)
			assert.Equal(t, 100, failedStats.bytes)

			succeededStats := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace2")][customValKey("customVal1")][jobStateKey("succeeded")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 3, succeededStats.count)
			assert.Equal(t, 50, succeededStats.bytes)

			abortedStats := stats1[partitionIDKey("partition1")][workspaceIDKey("workspace3")][customValKey("customVal1")][jobStateKey("aborted")][parameterFiltersKey("param2:value2")]
			assert.Equal(t, 2, abortedStats.count)
			assert.Equal(t, 20, abortedStats.bytes)
		})

		t.Run("merge complex hierarchy", func(t *testing.T) {
			stats1 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
								parameterFiltersKey("param2:value2"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param2", Value: "value2"}},
									count:      2,
									bytes:      20,
								},
								parameterFiltersKey(""): &UpdateJobStatusStats{
									parameters: ParameterFilterList{},
									count:      3,
									bytes:      25,
								},
							},
							jobStateKey("succeeded"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      10,
									bytes:      0,
								},
								parameterFiltersKey(""): &UpdateJobStatusStats{
									parameters: nil,
									count:      5,
									bytes:      0,
								},
							},
						},
						customValKey("customVal2"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param7:value7"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param7", Value: "value7"}},
									count:      4,
									bytes:      40,
								},
							},
						},
					},
					workspaceIDKey("workspace2"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param3:value3"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param3", Value: "value3"}},
									count:      1,
									bytes:      10,
								},
							},
						},
					},
				},
			}

			stats2 := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      3,
									bytes:      30,
								},
								parameterFiltersKey("param4:value4"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param4", Value: "value4"}},
									count:      1,
									bytes:      5,
								},
								parameterFiltersKey(""): &UpdateJobStatusStats{
									parameters: nil,
									count:      2,
									bytes:      15,
								},
							},
							jobStateKey("aborted"): {
								parameterFiltersKey("param5:value5"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param5", Value: "value5"}},
									count:      7,
									bytes:      70,
								},
							},
							jobStateKey("succeeded"): {
								parameterFiltersKey(""): &UpdateJobStatusStats{
									parameters: ParameterFilterList{},
									count:      3,
									bytes:      0,
								},
							},
						},
						customValKey("customVal2"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param7:value7"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param7", Value: "value7"}},
									count:      2,
									bytes:      20,
								},
								parameterFiltersKey("param8:value8"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param8", Value: "value8"}},
									count:      1,
									bytes:      10,
								},
							},
							jobStateKey("succeeded"): {
								parameterFiltersKey("param9:value9"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param9", Value: "value9"}},
									count:      6,
									bytes:      0,
								},
							},
						},
						customValKey("customVal3"): {
							jobStateKey("aborted"): {
								parameterFiltersKey("param10:value10"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param10", Value: "value10"}},
									count:      2,
									bytes:      80,
								},
							},
						},
					},
					workspaceIDKey("workspace3"): {
						customValKey("customVal1"): {
							jobStateKey("succeeded"): {
								parameterFiltersKey("param6:value6"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param6", Value: "value6"}},
									count:      15,
									bytes:      0,
								},
							},
						},
					},
				},
			}

			stats1.Merge(stats2)

			// Check merged values
			assert.Equal(t, 8, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")].count)   // 5 + 3
			assert.Equal(t, 130, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")].bytes) // 100 + 30

			// Check preserved values
			assert.Equal(t, 2, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param2:value2")].count)
			assert.Equal(t, 20, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param2:value2")].bytes)
			assert.Equal(t, 10, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("succeeded")][parameterFiltersKey("param1:value1")].count)
			assert.Equal(t, 1, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace2")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param3:value3")].count)

			// Check new values
			assert.Equal(t, 1, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param4:value4")].count)
			assert.Equal(t, 5, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param4:value4")].bytes)
			assert.Equal(t, 7, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("aborted")][parameterFiltersKey("param5:value5")].count)
			assert.Equal(t, 70, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("aborted")][parameterFiltersKey("param5:value5")].bytes)
			assert.Equal(t, 15, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace3")][customValKey("customVal1")][jobStateKey("succeeded")][parameterFiltersKey("param6:value6")].count)
			assert.Equal(t, 0, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace3")][customValKey("customVal1")][jobStateKey("succeeded")][parameterFiltersKey("param6:value6")].bytes)

			// Check merged empty parameter cases
			assert.Equal(t, 5, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("")].count)    // 3 + 2 (empty ParameterFilterList + nil parameters)
			assert.Equal(t, 40, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("")].bytes)   // 25 + 15
			assert.Equal(t, 8, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("succeeded")][parameterFiltersKey("")].count) // 5 + 3 (nil parameters + empty ParameterFilterList)
			assert.Equal(t, 0, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal1")][jobStateKey("succeeded")][parameterFiltersKey("")].bytes) // 0 + 0

			// Check merged custom values
			assert.Equal(t, 6, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal2")][jobStateKey("failed")][parameterFiltersKey("param7:value7")].count)     // 4 + 2
			assert.Equal(t, 60, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal2")][jobStateKey("failed")][parameterFiltersKey("param7:value7")].bytes)    // 40 + 20
			assert.Equal(t, 1, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal2")][jobStateKey("failed")][parameterFiltersKey("param8:value8")].count)     // new from stats2
			assert.Equal(t, 10, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal2")][jobStateKey("failed")][parameterFiltersKey("param8:value8")].bytes)    // new from stats2
			assert.Equal(t, 6, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal2")][jobStateKey("succeeded")][parameterFiltersKey("param9:value9")].count)  // new from stats2
			assert.Equal(t, 0, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal2")][jobStateKey("succeeded")][parameterFiltersKey("param9:value9")].bytes)  // new from stats2
			assert.Equal(t, 2, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal3")][jobStateKey("aborted")][parameterFiltersKey("param10:value10")].count)  // new from stats2
			assert.Equal(t, 80, stats1[partitionIDKey("partition1")][workspaceIDKey("workspace1")][customValKey("customVal3")][jobStateKey("aborted")][parameterFiltersKey("param10:value10")].bytes) // new from stats2
		})
	})

	t.Run("StatsByCustomValAndState", func(t *testing.T) {
		t.Run("empty stats", func(t *testing.T) {
			stats := updateJobStatusStats{}

			result := stats.StatsByCustomValAndState()

			assert.Empty(t, result)
		})

		t.Run("single workspace single state", func(t *testing.T) {
			stats := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
							},
						},
					},
				},
			}

			result := stats.StatsByCustomValAndState()

			require.Contains(t, result, customValKey("customVal1"))
			require.Contains(t, result[customValKey("customVal1")], jobStateKey("failed"))
			require.Contains(t, result[customValKey("customVal1")][jobStateKey("failed")], parameterFiltersKey("param1:value1"))

			aggregatedStats := result[customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 5, aggregatedStats.count)
			assert.Equal(t, 100, aggregatedStats.bytes)
			assert.Equal(t, "param1:value1", aggregatedStats.parameters.String())
		})

		t.Run("multiple workspaces same state same parameters", func(t *testing.T) {
			stats := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
							},
						},
					},
					workspaceIDKey("workspace2"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      3,
									bytes:      50,
								},
							},
						},
					},
				},
			}

			result := stats.StatsByCustomValAndState()

			require.Contains(t, result, customValKey("customVal1"))
			require.Contains(t, result[customValKey("customVal1")], jobStateKey("failed"))
			require.Contains(t, result[customValKey("customVal1")][jobStateKey("failed")], parameterFiltersKey("param1:value1"))

			aggregatedStats := result[customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 8, aggregatedStats.count)   // 5 + 3
			assert.Equal(t, 150, aggregatedStats.bytes) // 100 + 50
		})

		t.Run("multiple workspaces same state different parameters", func(t *testing.T) {
			stats := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
							},
						},
					},
					workspaceIDKey("workspace2"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param2:value2"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param2", Value: "value2"}},
									count:      3,
									bytes:      50,
								},
							},
						},
					},
				},
			}

			result := stats.StatsByCustomValAndState()

			require.Contains(t, result, customValKey("customVal1"))
			assert.Contains(t, result[customValKey("customVal1")], jobStateKey("failed"))
			assert.Contains(t, result[customValKey("customVal1")][jobStateKey("failed")], parameterFiltersKey("param1:value1"))
			assert.Contains(t, result[customValKey("customVal1")][jobStateKey("failed")], parameterFiltersKey("param2:value2"))
			stats1 := result[customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 5, stats1.count)
			assert.Equal(t, 100, stats1.bytes)

			stats2 := result[customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param2:value2")]
			assert.Equal(t, 3, stats2.count)
			assert.Equal(t, 50, stats2.bytes)
		})

		t.Run("multiple workspaces different states", func(t *testing.T) {
			stats := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
							},
							jobStateKey("succeeded"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      10,
									bytes:      0,
								},
							},
						},
					},
					workspaceIDKey("workspace2"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      3,
									bytes:      50,
								},
							},
							jobStateKey("aborted"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      2,
									bytes:      25,
								},
							},
						},
					},
				},
			}

			result := stats.StatsByCustomValAndState()

			assert.Contains(t, result, customValKey("customVal1"))

			assert.Contains(t, result[customValKey("customVal1")], jobStateKey("failed"))
			assert.Contains(t, result[customValKey("customVal1")], jobStateKey("succeeded"))
			assert.Contains(t, result[customValKey("customVal1")], jobStateKey("aborted"))

			// Check aggregated failed stats
			failedStats := result[customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 8, failedStats.count)   // 5 + 3
			assert.Equal(t, 150, failedStats.bytes) // 100 + 50

			// Check succeeded stats (only from workspace1)
			succeededStats := result[customValKey("customVal1")][jobStateKey("succeeded")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 10, succeededStats.count)
			assert.Equal(t, 0, succeededStats.bytes)

			// Check aborted stats (only from workspace2)
			abortedStats := result[customValKey("customVal1")][jobStateKey("aborted")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 2, abortedStats.count)
			assert.Equal(t, 25, abortedStats.bytes)
		})

		t.Run("multiple custom values same state", func(t *testing.T) {
			stats := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
							},
						},
						customValKey("customVal2"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param2:value2"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param2", Value: "value2"}},
									count:      3,
									bytes:      50,
								},
							},
						},
					},
					workspaceIDKey("workspace2"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      2,
									bytes:      40,
								},
							},
						},
						customValKey("customVal2"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param3:value3"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param3", Value: "value3"}},
									count:      1,
									bytes:      20,
								},
							},
						},
					},
				},
			}

			result := stats.StatsByCustomValAndState()

			require.Contains(t, result, customValKey("customVal1"))
			require.Contains(t, result, customValKey("customVal2"))
			require.Contains(t, result[customValKey("customVal1")], jobStateKey("failed"))
			require.Contains(t, result[customValKey("customVal2")], jobStateKey("failed"))

			// Check aggregated stats for customVal1 (should aggregate across workspaces)
			require.Contains(t, result[customValKey("customVal1")][jobStateKey("failed")], parameterFiltersKey("param1:value1"))
			cv1Stats := result[customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 7, cv1Stats.count)   // 5 + 2
			assert.Equal(t, 140, cv1Stats.bytes) // 100 + 40

			// Check aggregated stats for customVal2 (different parameters, no aggregation)
			require.Contains(t, result[customValKey("customVal2")][jobStateKey("failed")], parameterFiltersKey("param2:value2"))
			require.Contains(t, result[customValKey("customVal2")][jobStateKey("failed")], parameterFiltersKey("param3:value3"))
			cv2Stats1 := result[customValKey("customVal2")][jobStateKey("failed")][parameterFiltersKey("param2:value2")]
			assert.Equal(t, 3, cv2Stats1.count)
			assert.Equal(t, 50, cv2Stats1.bytes)
			cv2Stats2 := result[customValKey("customVal2")][jobStateKey("failed")][parameterFiltersKey("param3:value3")]
			assert.Equal(t, 1, cv2Stats2.count)
			assert.Equal(t, 20, cv2Stats2.bytes)
		})

		t.Run("multiple custom values different states", func(t *testing.T) {
			stats := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
							},
							jobStateKey("succeeded"): {
								parameterFiltersKey("param2:value2"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param2", Value: "value2"}},
									count:      10,
									bytes:      0,
								},
							},
						},
						customValKey("customVal2"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param3:value3"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param3", Value: "value3"}},
									count:      3,
									bytes:      50,
								},
							},
							jobStateKey("aborted"): {
								parameterFiltersKey("param4:value4"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param4", Value: "value4"}},
									count:      2,
									bytes:      30,
								},
							},
						},
					},
				},
			}

			result := stats.StatsByCustomValAndState()

			require.Contains(t, result, customValKey("customVal1"))
			require.Contains(t, result, customValKey("customVal2"))

			// Check customVal1 states
			require.Contains(t, result[customValKey("customVal1")], jobStateKey("failed"))
			require.Contains(t, result[customValKey("customVal1")], jobStateKey("succeeded"))
			cv1FailedStats := result[customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")]
			assert.Equal(t, 5, cv1FailedStats.count)
			assert.Equal(t, 100, cv1FailedStats.bytes)
			cv1SucceededStats := result[customValKey("customVal1")][jobStateKey("succeeded")][parameterFiltersKey("param2:value2")]
			assert.Equal(t, 10, cv1SucceededStats.count)
			assert.Equal(t, 0, cv1SucceededStats.bytes)

			// Check customVal2 states
			require.Contains(t, result[customValKey("customVal2")], jobStateKey("failed"))
			require.Contains(t, result[customValKey("customVal2")], jobStateKey("aborted"))
			cv2FailedStats := result[customValKey("customVal2")][jobStateKey("failed")][parameterFiltersKey("param3:value3")]
			assert.Equal(t, 3, cv2FailedStats.count)
			assert.Equal(t, 50, cv2FailedStats.bytes)
			cv2AbortedStats := result[customValKey("customVal2")][jobStateKey("aborted")][parameterFiltersKey("param4:value4")]
			assert.Equal(t, 2, cv2AbortedStats.count)
			assert.Equal(t, 30, cv2AbortedStats.bytes)
		})

		t.Run("complex aggregation", func(t *testing.T) {
			stats := updateJobStatusStats{
				partitionIDKey("partition1"): {
					workspaceIDKey("workspace1"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      5,
									bytes:      100,
								},
								parameterFiltersKey("param2:value2"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param2", Value: "value2"}},
									count:      2,
									bytes:      20,
								},
								parameterFiltersKey(""): &UpdateJobStatusStats{
									parameters: ParameterFilterList{},
									count:      3,
									bytes:      30,
								},
							},
							jobStateKey("succeeded"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      10,
									bytes:      0,
								},
								parameterFiltersKey("param3:value3"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param3", Value: "value3"}},
									count:      1,
									bytes:      0,
								},
							},
						},
						customValKey("customVal2"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param6:value6"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param6", Value: "value6"}},
									count:      3,
									bytes:      60,
								},
							},
							jobStateKey("succeeded"): {
								parameterFiltersKey("param7:value7"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param7", Value: "value7"}},
									count:      2,
									bytes:      0,
								},
							},
						},
					},
					workspaceIDKey("workspace2"): {
						customValKey("customVal1"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      3,
									bytes:      30,
								},
								parameterFiltersKey("param4:value4"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param4", Value: "value4"}},
									count:      1,
									bytes:      10,
								},
								parameterFiltersKey(""): &UpdateJobStatusStats{
									parameters: nil,
									count:      2,
									bytes:      20,
								},
							},
							jobStateKey("succeeded"): {
								parameterFiltersKey("param1:value1"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param1", Value: "value1"}},
									count:      7,
									bytes:      0,
								},
								parameterFiltersKey(""): &UpdateJobStatusStats{
									parameters: nil,
									count:      4,
									bytes:      0,
								},
							},
						},
						customValKey("customVal2"): {
							jobStateKey("failed"): {
								parameterFiltersKey("param6:value6"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param6", Value: "value6"}},
									count:      1,
									bytes:      15,
								},
							},
							jobStateKey("aborted"): {
								parameterFiltersKey("param8:value8"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param8", Value: "value8"}},
									count:      2,
									bytes:      40,
								},
							},
						},
					},
				},
				partitionIDKey("partition2"): {
					workspaceIDKey("workspace3"): {
						customValKey("customVal1"): {
							jobStateKey("aborted"): {
								parameterFiltersKey("param5:value5"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param5", Value: "value5"}},
									count:      4,
									bytes:      40,
								},
								parameterFiltersKey(""): &UpdateJobStatusStats{
									parameters: ParameterFilterList{},
									count:      6,
									bytes:      60,
								},
							},
						},
						customValKey("customVal3"): {
							jobStateKey("succeeded"): {
								parameterFiltersKey("param9:value9"): &UpdateJobStatusStats{
									parameters: ParameterFilterList{{Name: "param9", Value: "value9"}},
									count:      5,
									bytes:      0,
								},
							},
						},
					},
				},
			}

			result := stats.StatsByCustomValAndState()

			// Check failed state aggregation
			require.Contains(t, result, customValKey("customVal1"))
			require.Contains(t, result[customValKey("customVal1")], jobStateKey("failed"))
			assert.Contains(t, result[customValKey("customVal1")][jobStateKey("failed")], parameterFiltersKey("param1:value1"))
			assert.Contains(t, result[customValKey("customVal1")][jobStateKey("failed")], parameterFiltersKey("param2:value2"))
			assert.Contains(t, result[customValKey("customVal1")][jobStateKey("failed")], parameterFiltersKey("param4:value4"))
			assert.Contains(t, result[customValKey("customVal1")][jobStateKey("failed")], parameterFiltersKey(""))

			assert.Equal(t, 8, result[customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")].count)   // 5 + 3
			assert.Equal(t, 130, result[customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param1:value1")].bytes) // 100 + 30
			assert.Equal(t, 2, result[customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param2:value2")].count)
			assert.Equal(t, 20, result[customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param2:value2")].bytes)
			assert.Equal(t, 1, result[customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param4:value4")].count)
			assert.Equal(t, 10, result[customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("param4:value4")].bytes)
			assert.Equal(t, 5, result[customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("")].count)  // 3 + 2 (empty ParameterFilterList + nil parameters)
			assert.Equal(t, 50, result[customValKey("customVal1")][jobStateKey("failed")][parameterFiltersKey("")].bytes) // 30 + 20
			// Check succeeded state aggregation
			require.Contains(t, result[customValKey("customVal1")], jobStateKey("succeeded"))
			assert.Contains(t, result[customValKey("customVal1")][jobStateKey("succeeded")], parameterFiltersKey("param1:value1"))
			assert.Contains(t, result[customValKey("customVal1")][jobStateKey("succeeded")], parameterFiltersKey("param3:value3"))
			assert.Contains(t, result[customValKey("customVal1")][jobStateKey("succeeded")], parameterFiltersKey(""))

			assert.Equal(t, 17, result[customValKey("customVal1")][jobStateKey("succeeded")][parameterFiltersKey("param1:value1")].count) // 10 + 7
			assert.Equal(t, 0, result[customValKey("customVal1")][jobStateKey("succeeded")][parameterFiltersKey("param1:value1")].bytes)  // 0 + 0
			assert.Equal(t, 1, result[customValKey("customVal1")][jobStateKey("succeeded")][parameterFiltersKey("param3:value3")].count)
			assert.Equal(t, 0, result[customValKey("customVal1")][jobStateKey("succeeded")][parameterFiltersKey("param3:value3")].bytes)
			assert.Equal(t, 4, result[customValKey("customVal1")][jobStateKey("succeeded")][parameterFiltersKey("")].count) // only from workspace2 (nil parameters)
			assert.Equal(t, 0, result[customValKey("customVal1")][jobStateKey("succeeded")][parameterFiltersKey("")].bytes)
			// Check aborted state (no aggregation needed)
			require.Contains(t, result[customValKey("customVal1")], jobStateKey("aborted"))
			assert.Contains(t, result[customValKey("customVal1")][jobStateKey("aborted")], parameterFiltersKey("param5:value5"))
			assert.Contains(t, result[customValKey("customVal1")][jobStateKey("aborted")], parameterFiltersKey(""))

			assert.Equal(t, 4, result[customValKey("customVal1")][jobStateKey("aborted")][parameterFiltersKey("param5:value5")].count)
			assert.Equal(t, 40, result[customValKey("customVal1")][jobStateKey("aborted")][parameterFiltersKey("param5:value5")].bytes)
			assert.Equal(t, 6, result[customValKey("customVal1")][jobStateKey("aborted")][parameterFiltersKey("")].count) // only from workspace3 (empty ParameterFilterList)
			assert.Equal(t, 60, result[customValKey("customVal1")][jobStateKey("aborted")][parameterFiltersKey("")].bytes)
		})
	})
}
