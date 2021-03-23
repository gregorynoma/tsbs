package mongo

import (
	"encoding/gob"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/query"
)

func init() {
	// needed for serializing the mongo query to gob
	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
	gob.Register([]map[string]interface{}{})
	gob.Register(bson.M{})
	gob.Register(bson.D{})
	gob.Register([]bson.M{})
	gob.Register(time.Time{})
}

// NaiveDevops produces Mongo-specific queries for the devops use case.
type NaiveDevops struct {
	*BaseGenerator
	*devops.Core
}

// GroupByTime selects the MAX for numMetrics metrics under 'cpu',
// per minute for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT minute, max(metric1), ..., max(metricN)
// FROM cpu
// WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY minute ORDER BY minute ASC
func (d *NaiveDevops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	hostnames, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)

	pipelineQuery := []bson.M{
		{
			"$match": bson.M{
				"measurement": "cpu",
				"time": bson.M{
					"$gte": interval.Start(),
					"$lt":  interval.End(),
				},
				"tags.hostname": bson.M{
					"$in": hostnames,
				},
			},
		},
		{
			"$group": bson.M{
				"_id": bson.M{
					"$dateTrunc": bson.M{"date": "$time", "unit": "minute"},
				},
			},
		},
		{
			"$sort": bson.M{"_id": 1},
		},
	}
	resultMap := pipelineQuery[1]["$group"].(bson.M)
	for _, metric := range metrics {
		resultMap["max_"+metric] = bson.M{"$max": "$" + metric}
	}

	humanLabel := []byte(fmt.Sprintf("Mongo [NAIVE] %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange))
	q := qi.(*query.Mongo)
	q.HumanLabel = humanLabel
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s (%s)", humanLabel, interval.StartString(), q.CollectionName))
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in pseudo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname ORDER BY hour, hostname
func (d *NaiveDevops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)

	pipelineQuery := []bson.M{
		{
			"$match": bson.M{
				"measurement": "cpu",
				"time": bson.M{
					"$gte": interval.Start(),
					"$lt":  interval.End(),
				},
			},
		},
		{
			"$group": bson.M{
				"_id": bson.M{
					"time": bson.M{
						"$dateTrunc": bson.M{"date": "$time", "unit": "hour"},
					},
					"hostname": "$tags.hostname",
				},
			},
		},
		{
			"$sort": bson.D{{"_id.time", 1}, {"_id.hostname", 1}},
		},
	}
	resultMap := pipelineQuery[1]["$group"].(bson.M)
	for _, metric := range metrics {
		resultMap["avg_"+metric] = bson.M{"$avg": "$" + metric}
	}

	humanLabel := devops.GetDoubleGroupByLabel("Mongo [NAIVE]", numMetrics)
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s (%s)", humanLabel, interval.StartString(), q.CollectionName))
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT MAX(metric1), ..., MAX(metricN)
// FROM cpu WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour ORDER BY hour
func (d *NaiveDevops) MaxAllCPU(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.MaxAllDuration)
	hostnames, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)
	metrics := devops.GetAllCPUMetrics()

	pipelineQuery := []bson.M{
		{
			"$match": bson.M{
				"measurement": "cpu",
				"tags.hostname": bson.M{
					"$in": hostnames,
				},
				"time": bson.M{
					"$gte": interval.Start(),
					"$lt":  interval.End(),
				},
			},
		},
		{
			"$group": bson.M{
				"_id": bson.M{
					"$dateTrunc": bson.M{"date": "$time", "unit": "hour"},
				},
			},
		},
		{
			"$sort": bson.M{"_id": 1},
		},
	}
	resultMap := pipelineQuery[1]["$group"].(bson.M)
	for _, metric := range metrics {
		resultMap["max_"+metric] = bson.M{"$max": "$" + metric}
	}

	humanLabel := devops.GetMaxAllLabel("Mongo", nHosts)
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in pseudo-SQL:
//
// SELECT * FROM cpu
// WHERE usage_user > 90.0
// AND time >= '$TIME_START' AND time < '$TIME_END'
// AND (hostname = '$HOST' OR hostname = '$HOST2'...)
func (d *NaiveDevops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)
	metrics := devops.GetAllCPUMetrics()

	pipelineQuery := []bson.M{}

	// Must match in the documents that correspond to time, as well as optionally
	// filter on those with the correct host if nHosts > 0
	match := bson.M{
		"$match": bson.M{
			"measurement": "cpu",
			"time": bson.M{
				"$gte": interval.Start(),
				"$lt":  interval.End(),
			},
			"usage_user": bson.M{"$gt": 90.0},
		},
	}
	if nHosts > 0 {
		hostnames, err := d.GetRandomHosts(nHosts)
		panicIfErr(err)
		matchMap := match["$match"].(bson.M)
		matchMap["tags.hostname"] = bson.M{"$in": hostnames}
	}
	pipelineQuery = append(pipelineQuery, match)

	project := bson.M{
		"$project": bson.M{
			"_id":           0,
			"time":          1,
			"tags.hostname": 1,
		},
	}
	projectMap := project["$project"].(bson.M)
	for _, metric := range metrics {
		projectMap[metric] = 1
	}
	pipelineQuery = append(pipelineQuery, project)

	humanLabel, err := devops.GetHighCPULabel("Mongo", nHosts)
	panicIfErr(err)
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s (%s)", humanLabel, interval.StartString(), q.CollectionName))
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *NaiveDevops) LastPointPerHost(qi query.Query) {
	metrics := devops.GetAllCPUMetrics()

	pipelineQuery := []bson.M{
		{"$match": bson.M{"measurement": "cpu"}},
		{
			"$group": bson.M{
				"_id":       bson.M{"hostname": "$tags.hostname"},
				"last_time": bson.M{"$max": "$time"},
			},
		},
		{
			"$group": bson.M{
				"_id":   bson.M{"last_time": "$last_time"},
				"hosts": bson.M{"$addToSet": "$_id.hostname"},
			},
		},
		{
			"$lookup": bson.M{
				"from": "point_data",
				"let":  bson.M{"time": "$_id.last_time", "hosts": "$hosts"},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": []bson.M{
									{"$eq": []interface{}{"$time", "$$time"}},
									{"$in": []interface{}{"$tags.hostname", "$$hosts"}},
									{"$eq": []interface{}{"$measurement", "cpu"}},
								},
							},
						},
					},
					{
						"$project": bson.M{
							"time":          1,
							"tags.hostname": 1,
							"_id":           0,
						},
					},
				},
				"as": "results",
			},
		},
		{
			"$unwind": "$results",
		},
		{
			"$project": bson.M{
				"hostname":    "$results.tags.hostname",
				"result.time": "$results.time",
				"_id":         0,
			},
		},
	}

	lookupPipeline := pipelineQuery[3]["$lookup"].(bson.M)["pipeline"]
	lookupProjectMap := lookupPipeline.([]bson.M)[1]["$project"].(bson.M)
	projectMap := pipelineQuery[5]["$project"].(bson.M)
	for _, metric := range metrics {
		lookupProjectMap[metric] = 1
		projectMap["result."+metric] = "$results." + metric
	}

	humanLabel := "Mongo last row per host"
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s", humanLabel))
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause, that groups by a
// truncated date, orders by that date, and takes a limit, e.g. in pseudo-SQL:
//
// SELECT date_trunc('minute', time) AS t, MAX(cpu) FROM cpu
// WHERE time < '$TIME'
// GROUP BY t ORDER BY t DESC
// LIMIT $LIMIT
func (d *NaiveDevops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)
	interval, err := utils.NewTimeInterval(d.Interval.Start(), interval.End())
	if err != nil {
		panic(err.Error())
	}

	pipelineQuery := []bson.M{
		{
			"$match": bson.M{
				"measurement": "cpu",
				"time": bson.M{
					"$gte": interval.Start(),
					"$lt":  interval.End(),
				},
			},
		},
		{
			"$group": bson.M{
				"_id": bson.M{
					"$dateTrunc": bson.M{"date": "$time", "unit": "minute"},
				},
				"max_value": bson.M{"$max": "$usage_user"},
			},
		},
		{"$sort": bson.M{"_id": -1}},
		{"$limit": 5},
	}

	humanLabel := "Mongo max cpu over last 5 min-intervals (random end)"
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.EndString()))
}
