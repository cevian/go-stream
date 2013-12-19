package metrics

import (
	json "encoding/json"
	"github.com/cloudflare/go-stream/util"
	"github.com/cloudflare/go-stream/util/slog"
	zmq "github.com/pebbe/zmq3"
	metrics "github.com/rcrowley/go-metrics"
	"time"
)

var (
	Gm *util.StreamingMetrics // Main metrics object
)

const (
	DEFAULT_STATS_ADDR = "tcp://127.0.0.1:5450"
)

func InitDefaultMetrics() {
	Gm = util.NewStreamingMetrics(metrics.NewRegistry())
	metricsAddr := "tcp://127.0.0.1:5450"
	logPrefix := "Go-stream"
	go statsSender(&metricsAddr, &logPrefix)

}

func InitMetrics(metrics *util.StreamingMetrics, metricsAddr string, logPrefix string) {
	Gm = metrics
	go statsSender(&metricsAddr, &logPrefix)
}

type statsPkg struct {
	Name      string
	UpTime    int64
	OpMetrics map[string]interface{}
}

func statsSender(metricsAddr *string, processName *string) {
	rep, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		slog.Errorf("Stats Sender error: %v", err.Error())
		return
	}
	defer rep.Close()
	err = rep.Bind(*metricsAddr)
	if err != nil {
		slog.Errorf("Stats Sender error: %v", err.Error())
		return
	}

	slog.Infof("Stats sender, listening on %s", *metricsAddr)

	// Loop, printing the stats on request
	for {
		_, err := rep.Recv(0)
		if err != nil {
			slog.Errorf("%v", err.Error())
		} else {
			timestamp := time.Now().Unix() - Gm.StartTime
			dBag := statsPkg{*processName, timestamp, map[string]interface{}{}}
			for k, v := range Gm.OpGroups {
				dBag.OpMetrics[k] = map[string]int64{"Events": v.Events.Count(), "Errors": v.Errors.Count(), "Queue": v.QueueLength.Value()}
			}
			stats, err := json.Marshal(dBag)
			if err == nil {
				_, err = rep.SendBytes(stats, zmq.DONTWAIT)
				if err != nil {
					slog.Errorf("%v", err.Error())
				}
			}
		}
	}
}
