package gf_gdb_driver_taosSql

import (
	"database/sql"
	_ "errors"
	"fmt"
	_ "fmt"
	"github.com/gogf/gf/database/gdb"
	"github.com/gogf/gf/text/gregex"
	_"github.com/taosdata/driver-go/taosSql"
)

// taosSQLDriver is the driver for TDengine database.
type DriverTaosSQL struct {
	*gdb.Core
}

func (d *DriverTaosSQL) Open(config *gdb.ConfigNode) (*sql.DB, error) {
	var source string
	if config.LinkInfo != "" {
		source = config.LinkInfo
		// Custom changing the schema in runtime.
		if config.Name != "" {
			source, _ = gregex.ReplaceString(`/([\w\.\-]+)+`, "/"+config.Name, source)
		}
	} else {
		source = fmt.Sprintf(
			"%s:%s@/tcp(%s:%s)/%s?charset=%s&multiStatements=true&parseTime=true&loc=Local",
			config.User, config.Pass, config.Host, config.Port, config.Name, config.Charset,
		)
	}
	println("Open: %s", source)
	if db, err := sql.Open("taosSql", source); err == nil {
		return db, nil
	} else {
		return nil, err
	}
}

func (d *DriverTaosSQL) HandleSqlBeforeCommit(link gdb.Link, sql string, args []interface{}) (string, []interface{}) {
	return sql, args
}

// New creates and returns a database object for TDengine.
// It implements the interface of gdb.Driver for extra database driver installation.
func (d *DriverTaosSQL) New(core *gdb.Core, node *gdb.ConfigNode) (gdb.DB, error) {
	return &DriverTaosSQL{
		Core: core,
	}, nil
}

func (d *DriverTaosSQL) Tables(schema ...string) (tables []string, err error) {
	var result gdb.Result
	link, err := d.DB.GetSlave(schema...)
	if err != nil {
		return nil, err
	}
	result, err = d.DB.DoGetAll(link, `SHOW TABLES`)
	if err != nil {
		return
	}
	for _, m := range result {
		for _, v := range m {
			tables = append(tables, v.String())
		}
	}
	return
}
