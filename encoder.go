package csv

import (
	"fmt"
	"github.com/mozilla-services/heka/pipeline"
	"encoding/xml"
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"path"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type CsvEncoderConfig struct {
	ApiPath string `toml:"api_path"`
	Delimiter string `toml:"delimiter"`
}

type CsvEncoder struct {
	config *CsvEncoderConfig
	apiconfigs map[string]ApiConfig
}

type ApiArg struct {
	aName string
	aType string
	aValue string
	aKey bool
}

type ApiConfig struct {
	api string
	args []ApiArg
}

// for xml
type ApiFieldItem struct {
	Name string `xml:"Name,attr"`
	Type string `xml:"Type"`
	Dvalue string `xml:"Dvalue"`
	IsKey bool `xml:"IsKey"`
}

type ApiFields struct {
	XMLNAME xml.Name `xml:"Fields"`
	Items []ApiFieldItem `xml:"Field"`
}
// ---

func (en *CsvEncoder) ConfigStruct() interface{} {
	return &CsvEncoderConfig{
		ApiPath: "ApiConfig",
		Delimiter: "\001",
	}
}

func (en *CsvEncoder) Init(config interface{}) (err error) {
	en.config = config.(*CsvEncoderConfig)

	files, err := filepath.Glob(fmt.Sprintf("%s/fd-*.xml", en.config.ApiPath))
	if err != nil {
		return err
	}

	en.apiconfigs = make(map[string]ApiConfig)
	for _, file := range files {
		base := path.Base(file)
		api := base[3:len(base) - 4]

		b, e := ioutil.ReadFile(file)
		if e != nil {
			return e
		}
		fields := ApiFields{}
		if e = xml.Unmarshal(b, &fields); e != nil {
			return e
		}

		var args []ApiArg
		for line, i := range fields.Items {
			arg := ApiArg{}

			if len(i.Name) == 0 {
				return fmt.Errorf("xml: invalid name: %s, %d", file, line)
			}
			arg.aName = i.Name

			if typeOK(i.Type) {
				arg.aType = i.Type
			} else {
				return fmt.Errorf("xml: invalid type: %s, %d, %s", file, line, i.Name)
			}

			if len(i.Dvalue) == 0 {
				i.Dvalue = typeDefaultValue(i.Type)
			}
			arg.aValue = i.Dvalue

			if i.IsKey {
				arg.aKey = true
			} else {
				arg.aKey = false
			}

			args = append(args, arg)
		}

		en.apiconfigs[api] = ApiConfig{
			api: api,
			args: args,
		}
	}
	return nil
}

func (en *CsvEncoder) Encode(pack *pipeline.PipelinePack) (output []byte, err error) {
	fields := pack.Message.GetFields()

	var bpid, api_name, json_string string
	for _, f := range fields {
		switch f.GetName() {
		case "Bpid":
			bpid = f.GetValue().(string)
		case "ApiName":
			api_name = f.GetValue().(string)
		case "JsonString":
			json_string = f.GetValue().(string)
		}
	}

	jdata := make(map[string]interface{})
	err = json.Unmarshal([]byte(json_string), &jdata)
	if err != nil {
		return
	}

	apiconfig, ok := en.apiconfigs[api_name]
	if !ok {
		return nil, fmt.Errorf("No ApiConfig: %s", api_name)
	}

	var csv_arr []string
	for _, arg := range apiconfig.args {
		if arg.aName == "bpid" {
			csv_arr = append(csv_arr, bpid)
			continue
		}

		if value := jdata[arg.aName]; value != nil {
			if v, err := mapLogField(&arg, &value); err != nil {
				csv_arr = append(csv_arr, v)
				continue
			}
		}

		if arg.aKey {
			err = fmt.Errorf("invalid log field: %s", arg.aName)
		} else {
			switch arg.aType {
			case "int", "float":
				csv_arr = append(csv_arr, "0")
			case "datetime":
				csv_arr = append(csv_arr, "1970-01-01 00:00:00")
			case "str":
				csv_arr = append(csv_arr, "")
			default:
				continue
			}
		}
	}
	line := strings.Join(csv_arr, en.config.Delimiter)
	return []byte(line), nil
}

func init() {
	pipeline.RegisterPlugin("CsvEncoder", func() interface{} {
		return new(CsvEncoder)
	})
}

func typeOK(t string) bool {
	switch t {
	case "str", "int", "float", "datetime":
		return true
	default:
		return false
	}
}

func typeDefaultValue(t string) string {
	switch t {
	case "str":
		return ""
	case "int", "float", "datetime":
		return "0"
	default:
		return ""
	}
}

func mapLogField(arg *ApiArg, value *interface{}) (v string, err error) {
	v = arg.aValue

	switch arg.aType {
	case "str":
		v = mustStr(value)
	case "int":
		temp := mustInt(value)
		v = fmt.Sprintf("%d", temp)
	case "float":
		temp := mustFloat(value)
		v = fmt.Sprintf("%0.10f", temp)
	case "datetime":
		var t int64
		t = mustInt(value)
		if t > 0 {
			temp := time.Unix(t, 0).Local()
			v = temp.Format("2006-01-02 15:04:05")
		} else {
			v = "1970-01-01 00:00:00"
		}
	}

	return
}

func mustStr(value *interface{}) (s string) {
	switch reflect.ValueOf(*value).Kind() {
	case reflect.Bool:
		v := reflect.ValueOf(*value).Bool()
		if v {
			return "1"
		} else {
			return "0"
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.  Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v := reflect.ValueOf(*value).Int()
		return fmt.Sprintf("%d", v)
	case reflect.Float32, reflect.Float64:
		v := reflect.ValueOf(*value).Float()
		return fmt.Sprintf("%0.f", v)
	case reflect.String:
		return fmt.Sprintf("%s", reflect.ValueOf(*value).String())
	default:
		return ""
	}
	return ""
}

func mustInt(value *interface{}) (i int64) {
	switch reflect.ValueOf(*value).Kind() {
	case reflect.Bool:
		v := reflect.ValueOf(*value).Bool()
		if v {
			return 1
		} else {
			return 0
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.  Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return reflect.ValueOf(*value).Int()
	case reflect.Float32, reflect.Float64:
		v := reflect.ValueOf(*value).Float()
		return int64(v)
	case reflect.String:
		v := reflect.ValueOf(*value).String()
		if r, e := strconv.ParseInt(v, 10, 64); e != nil {
			if fr, fe := strconv.ParseFloat(v, 64); fe == nil {
				return int64(fr)
			}
			return 0
		} else {
			return r
		}
	default:
		return 0
	}
	return 0
}

func mustFloat(value *interface{}) (f float64) {
	switch reflect.ValueOf(*value).Kind() {
	case reflect.Bool:
		v := reflect.ValueOf(*value).Bool()
		if v {
			return 1
		} else {
			return 0
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.  Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v := reflect.ValueOf(*value).Int()
		return float64(v)
	case reflect.Float32, reflect.Float64:
		v := reflect.ValueOf(*value).Float()
		return v
	case reflect.String:
		v := reflect.ValueOf(*value).String()
		if r, e := strconv.ParseFloat(v, 64); e != nil {
			return 0
		} else {
			return r
		}
	default:
		return 0
	}
	return 0
}
