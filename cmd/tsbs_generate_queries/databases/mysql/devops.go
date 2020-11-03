package mysql

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/query"
)

const (
	errMoreItemsThanScale = "cannot get random permutation with more items than scale"
	roundToHour = "%Y-%m-%d %H:00:00"
	roundToMinute = "%Y-%m-%d %H:%i:00"
)

// TODO: Remove the need for this by continuing to bubble up errors
func panicIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

// Devops produces MySQL-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

// getHostWhereWithHostnames creates WHERE SQL statement for multiple hostnames.
// NOTE 'WHERE' itself is not included, just hostname filter clauses, ready to concatenate to 'WHERE' string
func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	hostnameClauses := []string{}
	if d.UseTags {
		for _, s := range hostnames {
			hostnameClauses = append(hostnameClauses, fmt.Sprintf("'%s'", s))
		}
		return fmt.Sprintf("tags_id IN (SELECT id FROM tags WHERE hostname IN (%s))", strings.Join(hostnameClauses, ","))
	} else {
		for _, s := range hostnames {
			hostnameClauses = append(hostnameClauses, fmt.Sprintf("hostname = '%s'", s))
		}
		combinedHostnameClause := strings.Join(hostnameClauses, " OR ")

		return "(" + combinedHostnameClause + ")"
	}
}

// getHostWhereString gets multiple random hostnames and creates a WHERE SQL statement for these hostnames.
func (d *Devops) getHostWhereString(nHosts int) string {
	hostnames, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames)
}

func (d *Devops) getSelectClausesAggMetrics(agg string, metrics []string) []string {
	selectClauses := make([]string, len(metrics))
	for i, m := range metrics {
		selectClauses[i] = fmt.Sprintf("%[1]s(%[2]s) as %[1]s_%[2]s", agg, m)
	}

	return selectClauses
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
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)
	if len(selectClauses) < 1 {
		panic(fmt.Sprintf("invalid number of select clauses: got %d", len(selectClauses)))
	}

	sql := fmt.Sprintf(`SELECT DATE_FORMAT(time, '%s') AS minute,
        %s
        FROM cpu
        WHERE %s AND time >= '%s' AND time < '%s'
        GROUP BY minute ORDER BY minute ASC`,
		roundToMinute,
		strings.Join(selectClauses, ", "),
		d.getHostWhereString(nHosts),
		interval.Start().Format(goTimeFmt),
		interval.End().Format(goTimeFmt))

	humanLabel := fmt.Sprintf("MySQL %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
// SELECT time_bucket('1 minute', time) AS t, MAX(cpu) FROM cpu
// WHERE time < '$TIME'
// GROUP BY t ORDER BY t DESC
// LIMIT $LIMIT
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)
	sql := fmt.Sprintf(`SELECT DATE_FORMAT(time, '%s') AS minute, max(usage_user)
        FROM cpu
        WHERE time < '%s'
        GROUP BY minute
        ORDER BY minute DESC
        LIMIT 5`,
		roundToMinute,
		interval.End().Format(goTimeFmt))

	humanLabel := "MySQL max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in pseudo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname ORDER BY hour
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)

	selectClauses := make([]string, numMetrics)
	meanClauses := make([]string, numMetrics)
	for i, m := range metrics {
		meanClauses[i] = "mean_" + m
		selectClauses[i] = fmt.Sprintf("avg(%s) as %s", m, meanClauses[i])
	}

	hostnameField := "hostname"
	joinStr := ""
	fromStr := "cpu"
	if d.UseTags {
		hostnameField = "tags.hostname"
		joinStr = "cpu.tags_id = tags.id AND"
		fromStr = "cpu, tags"
	}

	sql := fmt.Sprintf(`SELECT DATE_FORMAT(time, '%s') as hour, %s, %s
	FROM %s
	WHERE %s time >= '%s' AND time < '%s'
	GROUP BY hour, tags_id
        ORDER BY hour, tags_id`,
		roundToHour, hostnameField, strings.Join(selectClauses, ", "),
		fromStr, joinStr,
		interval.Start().Format(goTimeFmt),
		interval.End().Format(goTimeFmt))

/*
explain
SELECT DATE_FORMAT(time, '%Y-%m-%d %H:00:00') as hour, tags.hostname, avg(usage_user)
FROM cpu, tags
WHERE cpu.tags_id = tags.id AND time >= '2016-01-01 04:33:11.947779+00:00' AND time < '2016-01-01 16:33:11.947779+00:00'
GROUP BY hour, tags.id ORDER BY hour, tags.id;

	sql := fmt.Sprintf(`
        WITH cpu_avg AS (
          SELECT DATE_FORMAT(time, '%s') as hour, tags_id,
          %s
          FROM cpu
          WHERE time >= '%s' AND time < '%s'
          GROUP BY hour, tags_id
        )
        SELECT hour, %s, %s
        FROM cpu_avg
        %s
        ORDER BY hour, %s`,
		roundToHour,
		strings.Join(selectClauses, ", "),
		interval.Start().Format(goTimeFmt),
		interval.End().Format(goTimeFmt),
		hostnameField, strings.Join(meanClauses, ", "),
		joinStr, hostnameField)
*/
	humanLabel := devops.GetDoubleGroupByLabel("MySQL", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT MAX(metric1), ..., MAX(metricN)
// FROM cpu WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour ORDER BY hour
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.MaxAllDuration)

	metrics := devops.GetAllCPUMetrics()
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)

	sql := fmt.Sprintf(`SELECT DATE_FORMAT(time, '%s') AS hour,
        %s
        FROM cpu
        WHERE %s AND time >= '%s' AND time < '%s'
        GROUP BY hour ORDER BY hour`,
		roundToHour,
		strings.Join(selectClauses, ", "),
		d.getHostWhereString(nHosts),
		interval.Start().Format(goTimeFmt),
		interval.End().Format(goTimeFmt))

	humanLabel := devops.GetMaxAllLabel("MySQL", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	var sql string
	if d.UseTags {
	        sql = "SELECT * FROM tags, LATERAL (SELECT * FROM cpu WHERE cpu.tags_id = tags.id ORDER BY time DESC LIMIT 1) as lp ORDER by tags.hostname, lp.time DESC"
	} else {
		// sql = fmt.Sprintf(`SELECT DISTINCT ON (hostname) * FROM cpu ORDER BY hostname, time DESC`)
		panic("Not supported yet")
	}

	humanLabel := "MySQL last row per host"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in pseudo-SQL:
//
// SELECT * FROM cpu
// WHERE usage_user > 90.0
// AND time >= '$TIME_START' AND time < '$TIME_END'
// AND (hostname = '$HOST' OR hostname = '$HOST2'...)
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	var hostWhereClause string
	if nHosts == 0 {
		hostWhereClause = ""
	} else {
		hostWhereClause = fmt.Sprintf("AND %s", d.getHostWhereString(nHosts))
	}
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)

	sql := fmt.Sprintf(`SELECT * FROM cpu WHERE usage_user > 90.0 and time >= '%s' AND time < '%s' %s`,
		interval.Start().Format(goTimeFmt), interval.End().Format(goTimeFmt), hostWhereClause)

	humanLabel, err := devops.GetHighCPULabel("MySQL", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}
