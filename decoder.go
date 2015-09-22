package csv

import (
	"encoding/json"
	"fmt"
	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
	"github.com/satori/go.uuid"
	"io/ioutil"
	"strconv"
	"strings"
)

type CsvDecoderConfig struct {
	ApiMapsFile string `toml:"api_maps_file"`
}

type CsvDecoder struct {
	config     *CsvDecoderConfig
	runner     DecoderRunner
	helper     PluginHelper
	api_ts_map map[string]string
}

type Event struct {
	bpid        string
	api_name    string
	json_string string
	lts_at      string
	et_id       string
	uid         string
	logs        []Log
}

type Log struct {
	Lts_at    string `json:"lts_at"`
	Et_id     string `json:"et_id"`
	Uid       string `json:"uid"`
	Rand_id   string `json:"et_rk_id"`
	Arg_name  string `json:"arg_name"`
	Arg_value string `json:"arg_value"`
}

func (e *Event) GetRandId() string {
	u := uuid.NewV4().String()
	return strings.Join(strings.Split(u, "-"), "")
}

func (d *CsvDecoder) ConfigStruct() interface{} {
	return &CsvDecoderConfig{}
}

func (d *CsvDecoder) Init(config interface{}) error {
	d.config = config.(*CsvDecoderConfig)

	if len(d.config.ApiMapsFile) == 0 {
		return fmt.Errorf("api_maps_file not set")
	}
	b, err := ioutil.ReadFile(d.config.ApiMapsFile)
	if err != nil {
		return err
	}
	m := make(map[string]interface{})
	err = json.Unmarshal(b, &m)
	if err != nil {
		return err
	}
	if _, ok := m["api_ts_map"]; !ok {
		return fmt.Errorf("api_ts_map not set")
	}
	d.api_ts_map = make(map[string]string)
	for k, v := range m["api_ts_map"].(map[string]interface{}) {
		d.api_ts_map[k] = v.(string)
	}
	fmt.Println("111:", d.api_ts_map)
	return nil
}

func (d *CsvDecoder) getApiTs(api string) (ts_field string) {
	tapi := strings.TrimSpace(api)
	ts_field, ok := d.api_ts_map["lts_at"]
	if ok {
		return
	} else {
		if ts_field, ok = d.api_ts_map[tapi]; ok {
			return
		} else {
			ts_field = "lts_at"
		}
	}
	return
}

func (d *CsvDecoder) getLogAt(jdata map[string]interface{}, api string) (log_at int64) {
	ts_field := d.getApiTs(api)
	switch value := jdata[ts_field].(type) {
	case string:
		ts_int, err := strconv.Atoi(value)
		if err != nil || ts_int == 0 {
			ts_float, err := strconv.ParseFloat(value, 64)
			if err != nil {
				log_at = 0
			} else {
				log_at = int64(ts_float)
			}
		} else {
			log_at = int64(ts_int)
		}
	case int:
		log_at = int64(value)
	case float64:
		log_at = int64(value)
	case float32:
		log_at = int64(value)
	default:
		log_at = 0
	}
	return log_at
}

func (d *CsvDecoder) SetDecoderRunner(dr DecoderRunner) {
	d.runner = dr
}

func (d *CsvDecoder) Decode(pack *PipelinePack) (packs []*PipelinePack, err error) {
	fields := pack.Message.GetFields()

	event := new(Event)
	for _, f := range fields {
		switch f.GetName() {
		case "Bpid":
			event.bpid = f.GetValue().(string)
		case "ApiName":
			event.api_name = f.GetValue().(string)
		case "JsonString":
			event.json_string = f.GetValue().(string)
		}
	}

	jdata := make(map[string]interface{})
	err = json.Unmarshal([]byte(event.json_string), &jdata)
	if err != nil {
		return
	}
	log_at := d.getLogAt(jdata, event.api_name)
	message.NewInt64Field(pack.Message, "LogAt", log_at, "")

	if event.api_name != "by_event" {
		packs = []*PipelinePack{pack}
		return packs, nil
	}
	for key, value := range jdata {
		switch key {
		case "uid":
			event.uid = fmt.Sprintf("%v", value)
		case "et_id":
			event.et_id = fmt.Sprintf("%v", value)
		case "lts_at":
			event.lts_at = fmt.Sprintf("%v", value)
		default:
			event.logs = append(event.logs, Log{Arg_name: key, Arg_value: fmt.Sprintf("%v", value)})
		}
	}

	rid := event.GetRandId()
	packs = make([]*PipelinePack, len(event.logs))
	var i int = 0
	for _, log := range event.logs {
		log.Rand_id = rid
		log.Uid = event.uid
		log.Et_id = event.et_id
		log.Lts_at = event.lts_at

		p := d.runner.NewPack()
		pack.Message.Copy(p.Message)
		if f := p.Message.FindFirstField("ApiName"); f != nil {
			f.ValueString[0] = "by_event_args"
		}
		if f := p.Message.FindFirstField("JsonString"); f != nil {
			jstr, e := json.Marshal(log)
			if e != nil {
				fmt.Println(e)
				continue
			}
			fmt.Println("by_event: after:", string(jstr))
			f.ValueString[0] = string(jstr)
			packs[i] = p
			i++
		}
	}
	return
}

func init() {
	RegisterPlugin("CsvDecoder", func() interface{} {
		return new(CsvDecoder)
	})
}
