package slog

import (
	json "encoding/json"
	"fmt"
	"github.com/cloudflare/go-stream/util"
	"github.com/cloudflare/golog/logger"
	zmq "github.com/pebbe/zmq3"
	metrics "github.com/rcrowley/go-metrics"
	stdlogger "log"
	"os"
	"strings"
	"time"
)

var (
	defaultLog abstractLog            // the main logger object
	Gm         *util.StreamingMetrics // Main metrics object
)

const (
	DEFAULT_STATS_LOG_NAME   = "test"
	DEFAULT_STATS_LOG_LEVEL  = "debug"
	DEFAULT_STATS_LOG_PREFIX = "test"
	DEFAULT_STATS_ADDR       = "tcp://127.0.0.1:5450"
)

type Level int

const (
	Panic Level = iota
	Error
	Warn
	Info
	Debug
)

type abstractLog interface {
	Logf(level Level, format string, v ...interface{})
}

type StdLogger struct {
	log *stdlogger.Logger
}

func (l *StdLogger) Logf(level Level, format string, v ...interface{}) {
	if level >= Warn {
		l.log.Printf(format, v...)
	} else if level == Error {
		l.log.Printf(format, v...)
	} else if level == Panic {
		l.log.Panicf(format, v...)
	}
}

type CFLog struct {
	log       *logger.Logger
	logPrefix string
}

func (l *CFLog) Logf(level Level, format string, v ...interface{}) {
	lev := int(level) + 1
	l.log.Printf(logger.Level(lev), l.logPrefix, format, v...)
}

func Logf(level Level, format string, v ...interface{}) {
	if defaultLog != nil {
		defaultLog.Logf(level, format, v...)
	}
}

func Errorf(format string, v ...interface{}) { Logf(Error, format, v...) }
func Warnf(format string, v ...interface{})  { Logf(Warn, format, v...) }
func Infof(format string, v ...interface{})  { Logf(Info, format, v...) }
func Debugf(format string, v ...interface{}) { Logf(Debug, format, v...) }

func Fatalf(format string, v ...interface{}) {
	Logf(Panic, format, v...)

	os.Exit(1)
}

/*
// fatal: outputs a fatal startup error to STDERR, logs it to the
// logger if available and terminates the program
func fatal(l *logger.Logger, format string, v ...interface{}) {
	exit(1, l, format, v...)
}

// exit: outputs a startup message to STDERR, logs it to the
// logger if available and terminates the program with code
func exit(code int, l *logger.Logger, format string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", v...)

	if l != nil {
		l.Printf(logger.Levels.Error, LogPrefix, format, v...)
	}

	os.Exit(code)
}
*/
func logSetupFailure(format string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", v...)
	os.Exit(1)
}

func init() {
	InitSimpleLogger() //default logger
}

func InitSimpleLogger() {
	defaultLog = &StdLogger{stdlogger.New(os.Stderr, "", stdlogger.Lshortfile)}
}

func InitDefaultMetrics() {
	Gm = util.NewStreamingMetrics(metrics.NewRegistry())
	metricsAddr := "tcp://127.0.0.1:5450"
	logPrefix := "Go-stream"
	go statsSender(&metricsAddr, &logPrefix)

}

func Init(logName string, logLevel string, logPrefix string, metrics *util.StreamingMetrics, metricsAddr string,
	logAddress string, logNetwork string) {

	// Change logger level
	if err := logger.SetLogName(logName); err != nil {
		logSetupFailure("Cannot set log name for program")
	}

	// And set the logger to write to a custom socket.
	if logAddress != "" && logNetwork != "" {
		if err := logger.SetCustomSocket(logAddress, logNetwork); err != nil {
			logSetupFailure("Cannot set custom log socket program: %s %s %v", logAddress, logNetwork, err)
		}
	}

	if ll, ok := logger.CfgLevels[strings.ToLower(logLevel)]; !ok {
		logSetupFailure("Unsupported log level: " + logLevel)
	} else {
		glog := logger.New(ll)
		if glog == nil {
			logSetupFailure("Cannot start logger")
		}
		logPrefix := "[" + logPrefix + "] "
		cfl := CFLog{glog, logPrefix}
		defaultLog = &cfl
	}

	if metrics != nil {
		Gm = metrics
		go statsSender(&metricsAddr, &logPrefix)
	}
}

/*func Logf(level logger.Level, format string, v ...interface{}) {
	if glog != nil {
		glog.Printf(level, LogPrefix, format, v...)
	}
}

func Fatalf(format string, v ...interface{}) {
	fatal(glog, format, v)
}
*/
type statsPkg struct {
	Name      string
	UpTime    int64
	OpMetrics map[string]interface{}
}

func statsSender(metricsAddr *string, processName *string) {

	rep, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		Logf(Error, "Stats Sender error: %v", err.Error())
		return
	}
	defer rep.Close()
	err = rep.Bind(*metricsAddr)
	if err != nil {
		Logf(Error, "Stats Sender error: %v", err.Error())
		return
	}

	Logf(Info, "Stats sender, listening on %s", *metricsAddr)

	// Loop, printing the stats on request
	for {
		_, err := rep.Recv(0)
		if err != nil {
			Logf(Error, "%v", err.Error())
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
					Logf(Error, "%v", err.Error())
				}
			}
		}
	}
}
