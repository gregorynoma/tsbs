package mongo

import (
	"encoding/gob"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/query"
)

func init() {
	// needed for serializing the mongo query to gob
	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
	gob.Register([]map[string]interface{}{})
	gob.Register(bson.M{})
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
			"$project": bson.M{
				"_id": 0,
				"time_bucket": bson.M{
					"$dateSubtract": bson.M{
						"startDate": "$time",
						"unit":      "second",
						"amount":    bson.M{"$second": bson.M{"date": "$time"}},
					},
				},
			},
		},
	}

	projectMap := pipelineQuery[1]["$project"].(bson.M)
	for _, metric := range metrics {
		projectMap[metric] = 1
	}

	group := bson.M{
		"$group": bson.M{
			"_id": "$time_bucket",
		},
	}
	resultMap := group["$group"].(bson.M)
	for _, metric := range metrics {
		resultMap["max_"+metric] = bson.M{"$max": "$" + metric}
	}
	pipelineQuery = append(pipelineQuery, group)
	pipelineQuery = append(pipelineQuery, bson.M{"$sort": bson.M{"_id": 1}})

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
			"$project": bson.M{
				"_id": 0,
				"time_bucket": bson.M{
					"$dateSubtract": bson.M{
						"startDate": "$time",
						"unit":      "second",
						"amount":    bson.M{"$second": bson.M{"date": "$time"}},
					},
				},
				"tags.hostname": 1,
			},
		},
	}

	projectMap := pipelineQuery[1]["$project"].(bson.M)
	for _, metric := range metrics {
		projectMap[metric] = 1
	}

	// Add groupby operator
	group := bson.M{
		"$group": bson.M{
			"_id": bson.M{
				"time":     "$time_bucket",
				"hostname": "$tags.hostname",
			},
		},
	}
	resultMap := group["$group"].(bson.M)
	for _, metric := range metrics {
		resultMap["avg_"+metric] = bson.M{"$avg": "$" + metric}
	}
	pipelineQuery = append(pipelineQuery, group)
	pipelineQuery = append(pipelineQuery, bson.M{"$sort": bson.M{"_id.time": 1, "_id.hostname": 1}})

	humanLabel := devops.GetDoubleGroupByLabel("Mongo [NAIVE]", numMetrics)
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s (%s)", humanLabel, interval.StartString(), q.CollectionName))
}
