package mysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"strconv"
	"strings"
)

type Client struct {
	Address  string
	Port     string
	Username string
	Password string
	Database string
}

func NewClient(address string, port string, username string, password string, database string) *Client {

	return &Client{
		Address:  address,
		Port:     port,
		Username: username,
		Password: password,
		Database: database,
	}
}

func (client *Client) Name() string {
	return "mysql"
}

func (client *Client) Write(samples model.Samples) error {

	//Open mysql connections.
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/information_schema", client.Username, client.Password, client.Address, client.Port))
	if err != nil {
		return err
	}
	defer db.Close()

	//Parse schema properties.
	schemas, _ := client.ParseYml()
	err = client.Schemas(schemas)
	if err != nil {
		return err
	}

	//Open MySQL transaction.
	trx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, sample := range samples {
		metric := sample.Metric
		value := sample.Value
		timestamp := sample.Timestamp

		//only write schema tables.
		sampleName, ok := metric["__name__"]
		if !ok {
			log.Infoln("received invalid sample %v", sample)
			continue
		}

		persistentSchemas := schemas.Schemas
		if len(persistentSchemas) == 0 {
			log.Infoln("There's nothing to persistent schema")
			continue
		}

		var isNeed bool = false
		var isNeedSchema Schema
		for _, ps := range persistentSchemas {
			if strings.Compare(ps.SchemaName, string(sampleName)) == 0 {
				isNeed = true
				isNeedSchema = ps
				break
			}
		}

		if isNeed {
			schemaName := isNeedSchema.SchemaName
			schemaColumns := isNeedSchema.SchemaColumns

			var finalInsert = fmt.Sprintf("INSERT INTO %s.%s", client.Database, schemaName)
			var finalColumns = "("
			var finalValues = " VALUES("
			for key, val := range metric {
				for _, sc := range schemaColumns {
					column := sc.Column
					var suffix string = ","
					if strings.Compare(column, string(key)) == 0 {
						finalColumns = finalColumns + column + suffix
						switch sc.Type {
						case "string":
							finalValues = finalValues + "'" + string(val) + "'" + suffix
						case "long":
							finalValues = finalValues + string(val) + suffix
						case "float":
							finalValues = finalValues + string(val) + suffix
						}
					}
				}
			}
			finalColumns = finalColumns + "record_timestamp,value"
			finalColumns = finalColumns + ")"
			finalValues = finalValues + strconv.Itoa(int(timestamp)) + "," + strconv.FormatFloat(float64(value), 'f', 5, 32)
			finalValues = finalValues + ")"
			finalInsert = finalInsert + finalColumns + finalValues

			_, err := trx.Exec(finalInsert)
			if err != nil {
				log.Infoln("insert record failed %s", finalInsert)
				trx.Rollback()
			}
		}

	}
	err = trx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (client *Client) ParseYml() (*Schemas, error) {
	data, err := ioutil.ReadFile("schema.yml")
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	var conf = new(Schemas)
	if err := yaml.Unmarshal(data, conf); err != nil {
		fmt.Printf("err: %v\n", err)
		return nil, err
	}
	return conf, err
}

func (client *Client) Schemas(schemas *Schemas) error {

	//Do nothing if schemas length is zero.
	s := schemas.Schemas
	if len(s) == 0 {
		return nil
	}

	//Open mysql connection.
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/information_schema", client.Username, client.Password, client.Address, client.Port))
	if err != nil {
		return err
	}
	defer db.Close()

	//Create target database.
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s CHARACTER SET utf8", client.Database))
	if err != nil {
		return err
	}

	//Create table Schemas.
	for _, schema := range s {
		tableName := schema.SchemaName
		var tableSchema = "CREATE TABLE IF NOT EXISTS " + client.Database + "." + tableName + "("
		primaryKey := "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,"
		tableSchema = tableSchema + primaryKey
		columns := schema.SchemaColumns
		for index, column := range columns {
			columnName := column.Column
			columnType := column.Type
			columnSize := column.Size

			var suffix string
			if index == len(columns)-1 {
				suffix = ""
			} else {
				suffix = ","
			}

			switch columnType {
			case "string":
				columnSchema := fmt.Sprintf("%s VARCHAR(%d)%s", columnName, columnSize, suffix)
				tableSchema = tableSchema + columnSchema
			case "long":
				column_schema := fmt.Sprintf("%s BIGINT%s", columnName, suffix)
				tableSchema = tableSchema + column_schema
			case "float":
				column_schema := fmt.Sprintf("%s float%s", columnName, suffix)
				tableSchema = tableSchema + column_schema
			}
		}
		tableSchema = tableSchema + ") ENGINE=InnoDB CHARSET = utf8mb4"
		_, err := db.Exec(tableSchema)
		if err != nil {
			return err
		}
	}
	return nil
}
