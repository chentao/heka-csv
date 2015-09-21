package csv

import (
	"encoding/json"
	"fmt"
	. "github.com/mozilla-services/heka/pipeline"
	"github.com/satori/go.uuid"
	"strings"
)

type CsvDecoder struct {
	runner DecoderRunner
	helper PluginHelper
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

func (d *CsvDecoder) Init(config interface{}) error {
	return nil
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

	if event.api_name != "by_event" {
		packs = []*PipelinePack{pack}
		return packs, nil
	}

	jdata := make(map[string]interface{})
	err = json.Unmarshal([]byte(event.json_string), &jdata)
	if err != nil {
		return
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
