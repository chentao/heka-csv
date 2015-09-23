package csv

import (
	"fmt"
	. "github.com/mozilla-services/heka/pipeline"
	"sync"
)

type BylogFilter struct {
	counter Counter
}

type BylogFilterConfig struct {
}

type MetricType int

const (
	MTypeOK             MetricType = 0
	MTypeDelay          MetricType = 1
	MTypeDropFmtError   MetricType = 2
	MTypeDropTimeError  MetricType = 3
	MTypeDropJsonError  MetricType = 4
	MTypeDropOtherError MetricType = 5
	MTypeMaxNum         MetricType = 6
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
	this.counter.m = make(map[Key]Value)
	return nil
}

func (this *BylogFilter) Run(fr FilterRunner, h PluginHelper) (err error) {
	var pack *PipelinePack

	inChan := fr.InChan()
	for pack = range inChan {
		fields := pack.Message.GetFields()

		var log_at int64
		var metric_type int64
		var bpid, api_name, json_string string
		for _, f := range fields {
			switch f.GetName() {
			case "Bpid":
				bpid = f.GetValue().(string)
			case "ApiName":
				api_name = f.GetValue().(string)
			case "JsonString":
				json_string = f.GetValue().(string)
			case "LogAt":
				log_at = f.GetValue().(int64)
			case "Error":
				metric_type = f.GetValue().(int64)
			}
		}

		this.doMetric(bpid, api_name, MetricType(metric_type))

		fmt.Println("in filter:", log_at, bpid, api_name, json_string)
		fmt.Println("Counter:", this.counter.m)
		pack.Recycle()
	}

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
