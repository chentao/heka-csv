package csv

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/mozilla-services/heka/pipeline"
	"io/ioutil"
	"net/http"
	"errors"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type CsvEncoderConfig struct {
	ApiPath         string `toml:"api_path"`
	Delimiter       string `toml:"delimiter"`
	PrefixWithDate  bool   `toml:"prefix_with_date"`
	PrefixWithApi   bool   `toml:"prefix_with_apiname"`
	ApiMapsFile     string `toml:"api_maps_file"`
	LocationSvrAddr string `toml:"location_svr_addr"`
}

type CsvEncoder struct {
	config                 *CsvEncoderConfig
	apiconfigs             map[string]ApiConfig
	api_ts_map             map[string]string
	api_ip_location_map    map[string]string
	api_phone_location_map map[string]map[string]string
}

type ApiArg struct {
	aName  string
	aType  string
	aValue string
	aKey   bool
}

type ApiConfig struct {
	api  string
	args []ApiArg
}

// for xml
type ApiFieldItem struct {
	Name   string `xml:"Name,attr"`
	Type   string `xml:"Type"`
	Dvalue string `xml:"Dvalue"`
	IsKey  bool   `xml:"IsKey"`
}

type ApiFields struct {
	XMLNAME xml.Name       `xml:"Fields"`
	Items   []ApiFieldItem `xml:"Field"`
}

// ---

func (en *CsvEncoder) ConfigStruct() interface{} {
	return &CsvEncoderConfig{
		ApiPath:   "ApiConfig",
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
		api := base[3 : len(base)-4]

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
			api:  api,
			args: args,
		}
	}

	if len(en.config.ApiMapsFile) == 0 {
		return fmt.Errorf("api_maps_file not set")
	}
	b, err := ioutil.ReadFile(en.config.ApiMapsFile)
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
	if _, ok := m["api_ip_location_map"]; !ok {
		return fmt.Errorf("api_ip_location_map not set")
	}
	if _, ok := m["api_phone_location_map"]; !ok {
		return fmt.Errorf("api_phone_location_map not set")
	}
	en.api_ts_map = make(map[string]string)
	for k, v := range m["api_ts_map"].(map[string]interface{}) {
		en.api_ts_map[k] = v.(string)
	}
	en.api_ip_location_map = make(map[string]string)
	for k, v := range m["api_ip_location_map"].(map[string]interface{}) {
		en.api_ip_location_map[k] = v.(string)
	}
	en.api_phone_location_map = make(map[string]map[string]string)
	for k, v := range m["api_phone_location_map"].(map[string]interface{}) {
		en.api_phone_location_map[k] = make(map[string]string)
		for k2, v2 := range v.(map[string]interface{}) {
			en.api_phone_location_map[k][k2] = v2.(string)
		}
	}
	fmt.Println("111:", en.api_ts_map)
	fmt.Println("222:", en.api_ip_location_map)
	fmt.Println("333:", en.api_phone_location_map)
	return nil
}

func (en *CsvEncoder) Encode(pack *pipeline.PipelinePack) (output []byte, err error) {
	fields := pack.Message.GetFields()

	var log_at int64
	var log_date string
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

	log_at = en.getLogAt(jdata, api_name)
	log_date = time.Unix(log_at, 0).Format("2006-01-02")

	apiconfig, ok := en.apiconfigs[api_name]
	if !ok {
		return nil, fmt.Errorf("No ApiConfig: %s", api_name)
	}

	jdata, err = en.jsonAddLocationInfo(jdata, api_name)
	if err != nil {
		fmt.Println(err)
	}

	var csv_arr []string
	if en.config.PrefixWithDate {
		csv_arr = append(csv_arr, log_date)
	}
	if en.config.PrefixWithApi {
		csv_arr = append(csv_arr, api_name)
	}
	for _, arg := range apiconfig.args {
		if arg.aName == "bpid" {
			csv_arr = append(csv_arr, bpid)
			continue
		}

		if value := jdata[arg.aName]; value != nil {
			if v, e := mapLogField(&arg, &value); e == nil {
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

func (en *CsvEncoder) jsonAddLocationInfo(jmap map[string]interface{}, api string) (map[string]interface{}, error) {
	if len(en.config.LocationSvrAddr) == 0 {
		return jmap, nil
	}

	if ip_key, ok := en.api_ip_location_map[api]; ok {
		if ip_value, ok := jmap[ip_key]; ok {
			switch ip := ip_value.(type) {
			case string:
				ipinfo, err := queryLocationInfo(en.config.LocationSvrAddr, "ip", ip)
				if err == nil { /* Query OK */
					jmap["x_country"] = ipinfo["country"].(string)
					jmap["x_province"] = ipinfo["province"].(string)
					jmap["x_city"] = ipinfo["city"].(string)
					jmap["x_country_code"] = ipinfo["country_code"].(string)

					return jmap, nil
				} else {
					return jmap, fmt.Errorf("queryLocationInfo fail: %v", err)
				}
			default:
			}
		}
	}

	if key_info, ok := en.api_phone_location_map[api]; ok {
		if where, ok := key_info["where"]; ok {
			if equal_to, ok := key_info["equal_to"]; ok {
				if where_value, ok := jmap[where]; ok {
					switch value := where_value.(type) {
					case string:
						if value == equal_to {
							if transform, ok := key_info["transform"]; ok {
								phone_info, err := queryLocationInfo(en.config.LocationSvrAddr, "phone", jmap[transform].(string))
								if err == nil {
									jmap["x_country"] = phone_info["country"].(string)
									jmap["x_province"] = phone_info["province"].(string)
									jmap["x_city"] = phone_info["city"].(string)
									jmap["x_country_code"] = phone_info["country_code"].(string)

									return jmap, nil
								} else {
									return jmap, fmt.Errorf("queryLocationInfo fail: %v", err)
								}
							}
						}
					default:
					}
				}
			}
		}
	}

	return jmap, nil
}

func (en *CsvEncoder) getLogAt(jdata map[string]interface{}, api string) (log_at int64) {
	ts_field := en.getApiTs(api)
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

func (en *CsvEncoder) getApiTs(api string) (ts_field string) {
	tapi := strings.TrimSpace(api)
	ts_field, ok := en.api_ts_map["lts_at"]
	if ok {
		return
	} else {
		if ts_field, ok = en.api_ts_map[tapi]; ok {
			return
		} else {
			ts_field = "lts_at"
		}
	}
	return
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

	return v, nil
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
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
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
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
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
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
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

func queryLocationInfo(addr string, query string, target string) (map[string]interface{}, error) {
	url := "http://" + addr + "/location/" + query + "=" + target
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var obj map[string]interface{}
	err = json.Unmarshal(body, &obj)
	if err != nil {
		return nil, err
	}

	switch c := obj["code"].(type) {
	case float64:
		if c != 0 {
			return nil, fmt.Errorf("code not 0: %s: %s", obj["message"].(string), target)
		}
		return obj, nil
	default:
		return nil, errors.New("code type unknown")
	}

	return nil, errors.New("query location info fail")
}
