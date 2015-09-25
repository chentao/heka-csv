package csv

import (
	"encoding/json"
	"fmt"
	. "github.com/mozilla-services/heka/pipeline"
	"os"
	"strings"
	"sync"
	"time"
)

type BylogFilter struct {
	config   *BylogFilterConfig
	counter  Counter
	hostname string
	pid      int
	fr       FilterRunner
	h        PluginHelper
}

type BylogFilterConfig struct {
	MetricInterval int    `toml:"metric_interval"`
	OutputFormat   string `toml:"output_format"`
	SendNullMetric bool   `toml:"send_null_metric"`
}

type MetricType int

const (
	MTypeOK             MetricType = 0
	MTypeDelay          MetricType = 1
	MTypeDropTimeError  MetricType = 2
	MTypeDropJsonError  MetricType = 3
	MTypeDropOtherError MetricType = 4
	MTypeMaxNum         MetricType = 5
)

type Key struct {
	bpid, api string
}

type Value struct {
	counts []int
}

type Counter struct {
	sync.RWMutex
	m map[Key]Value
}

func (this *BylogFilter) ConfigStruct() interface{} {
	return &BylogFilterConfig{}
}

func (this *BylogFilter) Init(config interface{}) error {
	this.config = config.(*BylogFilterConfig)

	switch this.config.OutputFormat {
	case "csv", "json":
		LogInfo.Println("Output format is", this.config.OutputFormat)
	default:
		return fmt.Errorf("Output format must be csv or json")
	}

	this.counter.m = make(map[Key]Value)
	this.hostname, _ = os.Hostname()
	this.pid = os.Getpid()

	go func() {
		var timer *time.Ticker
		for {
			now := time.Now()
			truncated := now.Add(time.Duration(1) * time.Hour).Truncate(time.Hour)

			var sleep time.Duration
			if this.config.MetricInterval == 0 {
				sleep = truncated.Sub(now)
			} else {
				sleep = time.Duration(this.config.MetricInterval) * time.Second
			}
			timer = time.NewTicker(sleep)
			LogInfo.Printf("Now is %s, going to sleep %s\n", now.String(), sleep.String())

			select {
			case <-timer.C:
				this.cleanMetricCounter(false)
			}
			timer.Stop()
		}
	}()

	return nil
}

func (this *BylogFilter) Run(fr FilterRunner, h PluginHelper) (err error) {
	this.fr = fr
	this.h = h

	var pack *PipelinePack
	inChan := fr.InChan()
	for pack = range inChan {
		fields := pack.Message.GetFields()

		var metric_type int64
		var bpid, api_name string
		for _, f := range fields {
			switch f.GetName() {
			case "Bpid":
				bpid = f.GetValue().(string)
			case "ApiName":
				api_name = f.GetValue().(string)
			case "Error":
				metric_type = f.GetValue().(int64)
			}
		}

		this.doMetric(bpid, api_name, MetricType(metric_type))
		pack.Recycle()
	}

	this.cleanMetricCounter(true)
	return
}

func init() {
	RegisterPlugin("BylogFilter", func() interface{} {
		return new(BylogFilter)
	})
}

func (f *BylogFilter) doMetric(bpid, api string, t MetricType) {
	if t >= MTypeMaxNum {
		return
	}

	f.counter.Lock()
	if v, ok := f.counter.m[Key{bpid, api}]; ok {
		v.counts[t]++
		f.counter.m[Key{bpid, api}] = v
	} else {
		new_value := Value{make([]int, MTypeMaxNum)}
		new_value.counts[t]++
		f.counter.m[Key{bpid, api}] = new_value
	}
	f.counter.Unlock()
}

func (f *BylogFilter) cleanMetricCounter(exiting bool) {
	num := 0
	f.counter.Lock()
	for k, v := range f.counter.m {
		d, _ := time.ParseDuration("-1h")
		t := time.Now().Add(d)
		if exiting == true {
			t = time.Now()
		}

		var point string
		if f.config.OutputFormat == "json" {
			type M struct {
				Lts_at  int64  `json:"lts_at"`
				Time    string `json:"time"`
				MBpid   string `json:"mbpid"`
				MApi    string `json:"mapi"`
				Host    string `json:"host"`
				CountIn string `json:"count_in"`
			}
			clock := time.Now().Unix()
			m := M{
				Lts_at:  clock,
				Time:    t.Truncate(time.Hour).Format("2006-01-02 15:04:05.999999999"),
				MBpid:   k.bpid,
				MApi:    k.api,
				Host:    fmt.Sprintf("%s:%d", f.hostname, f.pid),
				CountIn: fmt.Sprintf("%d", v.counts[MTypeOK]),
			}
			jstr, err := json.Marshal(m)
			if err != nil {
				f.fr.LogError(err)
				continue
			}

			point = fmt.Sprintf("%s|%d|%s\t%s", "BBBEEEE000001111112222222FFFFFFF", clock, "bylog_metrics", jstr)
		} else {
			var arr []string
			arr = append(arr, t.Truncate(time.Hour).Format("2006-01-02 15:04:05.999999999"))
			arr = append(arr, k.bpid)
			arr = append(arr, k.api)
			arr = append(arr, fmt.Sprintf("%s:%d", f.hostname, f.pid))
			for _, count := range v.counts {
				arr = append(arr, fmt.Sprintf("%d", count))
			}
			point = strings.Join(arr, ",")
		}

		num++
		f.deliverMetric(point + "\n")
	}
	f.counter.m = make(map[Key]Value)
	if f.config.SendNullMetric == true {
		new_value := Value{make([]int, MTypeMaxNum)}
		f.counter.m[Key{"", ""}] = new_value
	}
	f.counter.Unlock()

	LogInfo.Println("This hour: num of points in counter:", num)
}

func (f *BylogFilter) deliverMetric(point string) {
	const msgType = "BylogMetrics"

	pack := f.h.PipelinePack(0)
	if pack == nil {
		LogError.Println("exceeded MaxMsgLoops =", f.h.PipelineConfig().Globals.MaxMsgLoops)
		return
	}
	pack.Message.SetLogger(f.fr.Name())
	pack.Message.SetType(msgType)
	pack.Message.SetPayload(point)
	f.fr.Inject(pack)
}
