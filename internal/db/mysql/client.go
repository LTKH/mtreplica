package mysql

import (
    //"os"
    "fmt"
    //"log"
    //"sync"
    //"time"
    //"errors"
    "context"
    //"encoding/json"
    "database/sql"
    "github.com/go-sql-driver/mysql"
    "github.com/go-mysql-org/go-mysql/replication"
    "github.com/ltkh/mtreplica/internal/config"
)

type Client struct {
    config     mysql.Config
    client     *sql.DB
    dbname     string
}

type Table struct {
    Schema     string
    Table      string
    Timestamp  int64
}

func New(n config.Node) (*Client, error) {

    cfg := mysql.Config{
        User:      n.User,
        Passwd:    n.Password,
        Net:       "tcp",
        Addr:      fmt.Sprintf("%v:%v", n.Host, n.Port), // Хост и порт здесь
        //ParseTime: true,
        //DBName:    n.Name,
    }

    conn, err := sql.Open("mysql", cfg.FormatDSN())
    if err != nil {
        return nil, err
    }

    client := &Client{
        config: cfg,
        client: conn,
        dbname: n.Name,
    }

    return client, nil
}

func (db *Client) GetTables() ([]Table, error) {
    var tables []Table

	rows, err := db.client.Query("SELECT UNIX_TIMESTAMP(UPDATE_TIME), TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES")
	if err != nil {
		return tables, err
	}
	defer rows.Close()
	
	for rows.Next() {
        var lastUpdate sql.NullInt64
		var schemaName, tableName string
		if err := rows.Scan(&lastUpdate, &schemaName, &tableName); err != nil {
			return tables, err
		}

        if lastUpdate.Valid {
            tables = append(tables, Table{Schema: schemaName, Table: tableName, Timestamp: lastUpdate.Int64})
        } else {
            tables = append(tables, Table{Schema: schemaName, Table: tableName, Timestamp: 0})
        }
	}

    return tables, nil
}

func (db *Client) SetEvents(events []config.Event) (error) {

    client, _ := sql.Open("mysql", db.config.FormatDSN())
    ctx := context.Background()

    // 1. Резервируем ОДНО конкретное соединение из пула
	conn, err := client.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close() // Возвращаем в пул после работы

    // 2. Отключаем лог (вне транзакции, но в этой сессии)
	_, err = conn.ExecContext(ctx, "SET sql_log_bin = 0")
	if err != nil {
		return err
	}

    // 3. Выполняем запросы (они НЕ попадут в бинлог)
	// Можно выполнить несколько ExecContext подряд
	//_, err = conn.ExecContext(ctx, "UPDATE users SET status = 'hidden' WHERE id = 10")
	//if err != nil {
	//	return err
	//}
    for _, event := range events {
        switch event.Type {
        case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
            //eventsTotal.With(prometheus.Labels{"server_id": fmt.Sprintf("%v", n.ServerID), "schema": schema, "table": table, "type": "insert"}).Inc()
            fmt.Printf("%s - %v\n", event.Type, event.Rows)
        
        case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
            //eventsTotal.With(prometheus.Labels{"server_id": fmt.Sprintf("%v", n.ServerID),"schema": schema, "table": table, "type": "update"}).Inc()
            fmt.Printf("%s - %v\n", event.Type, event.Rows)
        
        case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
            //eventsTotal.With(prometheus.Labels{"server_id": fmt.Sprintf("%v", n.ServerID), "schema": schema, "table": table, "type": "delete"}).Inc()
            fmt.Printf("%s - %v\n", event.Type, event.Rows)
        }
    }

    // 4. (Опционально) Включаем лог обратно, если планируете 
	// использовать это же соединение дальше для обычных задач
	//_, err = conn.ExecContext(ctx, "SET sql_log_bin = 1")

    return nil
}