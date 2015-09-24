package csv

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	. "github.com/mozilla-services/heka/pipeline"
	"strings"
	"time"
)

type PgOutput struct {
	config  *PgOutputConfig
	db_conn *sql.DB
}

type PgOutputConfig struct {
	PgURL     string `toml:"pg_url"`
	PgTable   string `toml:"pg_table"`
	PgSchema  string `toml:"pg_schema"`
	BatchSize int    `toml:"batch_size"`
}

func (po *PgOutput) ConfigStruct() interface{} {
	return &PgOutputConfig{
		BatchSize: 1000,
	}
}

func (po *PgOutput) Init(config interface{}) (err error) {
	po.config = config.(*PgOutputConfig)

	if len(po.config.PgURL) == 0 {
		return fmt.Errorf("pg_url not set")
	}
	if len(po.config.PgTable) == 0 {
		return fmt.Errorf("pg_table not set")
	}
	if len(po.config.PgSchema) == 0 {
		return fmt.Errorf("pg_schema not set")
	}
	po.db_conn, err = sql.Open("postgres", po.config.PgURL)
	if err != nil {
		return
	}

	LogInfo.Println("Connect to db success.")
	return
}

func (po *PgOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	if or.Encoder() == nil {
		return errors.New("Encoder required")
	}

	var outBytes []byte
	var e error
	var i int = 0
	var timer *time.Ticker

	timer = time.NewTicker(time.Duration(60) * time.Second)
	points := make([]string, po.config.BatchSize)
	for {
		select {
		case pack, ok := <-or.InChan():
			if !ok {
				if i > 0 && i <= 1000 {
					e = po.sendPoints(points[:i])
					if e != nil {
						or.LogError(fmt.Errorf("fail send points: %d, %v", i, e))
					} else {
						or.LogMessage(fmt.Sprintf("send metrics to pg success(by shutdown): i = %d", i))
					}
				}
				return
			}
			if outBytes, e = or.Encode(pack); e != nil {
				or.LogError(fmt.Errorf("Error encoding message: %s", e.Error()))
			} else if outBytes != nil {
				points[i] = string(outBytes)
				i++
				if i >= po.config.BatchSize {
					e = po.sendPoints(points[:i])
					if e != nil {
						or.LogError(fmt.Errorf("fail send points: %d, %v", i, e))
					} else {
						or.LogMessage(fmt.Sprintf("send metrics to pg success: i = %d", i))
					}
					i = 0
					timer = time.NewTicker(time.Duration(30) * time.Second)
				}
			}
			pack.Recycle()
		case <-timer.C:
			if i > 0 && i <= 1000 {
				e = po.sendPoints(points[:i])
				if e != nil {
					or.LogError(fmt.Errorf("fail send points: %d, %v", i, e))
				} else {
					or.LogMessage(fmt.Sprintf("send metrics to pg success(by timer): i = %d", i))
				}
				i = 0
			}
		}
	}
	return
}

func (po *PgOutput) sendPoints(points []string) error {
	txn, err := po.db_conn.Begin()
	if err != nil {
		return err
	}

	stmt, err := txn.Prepare(pq.CopyInSchema(po.config.PgSchema, po.config.PgTable,
		"ftime", "fbpid", "fapi", "fhost",
		"fcount_in",
		"fcount_delay",
		"fcount_timeerr",
		"fcount_jsonerr",
		"fcount_othererr"))
	if err != nil {
		return err
	}

	for _, p := range points {
		fields := strings.Split(p, ",")
		_, err = stmt.Exec(
			fields[0],
			fields[1],
			fields[2],
			fields[3],
			fields[4],
			fields[5],
			fields[6],
			fields[7],
			fields[8],
		)
		if err != nil {
			return err
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	err = txn.Commit()
	if err != nil {
		return err
	}
	return nil
}

func init() {
	RegisterPlugin("PgOutput", func() interface{} {
		return new(PgOutput)
	})
}
