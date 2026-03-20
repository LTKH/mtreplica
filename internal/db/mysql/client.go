package mysql

import (
    //"os"
    "fmt"
    //"log"
    //"sync"
    //"time"
    //"errors"
    //"bytes"
    "strings"
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
}

func New(n config.Node) (*Client, error) {

    cfg := mysql.Config{
        User:      n.User,
        Passwd:    n.Password,
        Net:       "tcp",
        Addr:      fmt.Sprintf("%v:%v", n.Host, n.Port), // Хост и порт здесь
        //ParseTime: true,
        //DBName:    n.Name,
        AllowNativePasswords: true,
    }

    conn, err := sql.Open("mysql", cfg.FormatDSN())
    if err != nil {
        return nil, err
    }

    client := &Client{
        config: cfg,
        client: conn,
    }

    return client, nil
}

func (db *Client) GetTablesInfo(schema string) (map[string]config.Table, error) {
    tables := map[string]config.Table{}

    rowsTables, err := db.client.Query("SELECT UNIX_TIMESTAMP(UPDATE_TIME), TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES")
    if err != nil {
        return tables, err
    }
    defer rowsTables.Close()
	
    for rowsTables.Next() {
        var lastUpdate sql.NullInt64
        var schemaName, tableName string
        if err := rowsTables.Scan(&lastUpdate, &schemaName, &tableName); err != nil {
            return tables, err
        }

        if schema != "" && schema != schemaName {
            continue
        }

        name := fmt.Sprintf("%s.%s", schemaName, tableName)
        table, found := tables[name]
        if !found {
            table = config.Table{
                Timestamp: 0,
                PrimaryKeys: make(map[string]int),
            }
        }

        if lastUpdate.Valid {
            table.Timestamp = lastUpdate.Int64
        }

        tables[name] = table
    }

    rowsColumns, err := db.client.Query("SELECT COLUMN_NAME, ORDINAL_POSITION, TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION")
    if err != nil {
        return tables, err
    }
    defer rowsColumns.Close()
    
    for rowsColumns.Next() {
        var ordinalPosition int
        var columnName, schemaName, tableName string
        if err := rowsColumns.Scan(&columnName, &ordinalPosition, &schemaName, &tableName); err != nil {
            return tables, err
        }

        if schema != "" && schema != schemaName {
            continue
        }

        name := fmt.Sprintf("%s.%s", schemaName, tableName)
        table, found := tables[name]
        if !found {
            table = config.Table{
                Timestamp: 0,
                PrimaryKeys: make(map[string]int),
            }
        }

        table.PrimaryKeys[columnName] = ordinalPosition

        tables[name] = table
	}

    return tables, nil
}

func (db *Client) SetEvents(tables map[string]config.Table, events []config.Event) (error) {

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

    for _, event := range events {
        name := fmt.Sprintf("%s.%s", event.Schema, event.Table)

        switch event.Type {
        case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
            if len(event.Rows[0]) > 0 {
                cols := make([]string, len(event.Rows[0]))
                for i := range cols {cols[i] = "?"}

                _, err = conn.ExecContext(ctx, fmt.Sprintf("REPLACE INTO %s VALUES (%s)", name, strings.Join(cols, ",")), event.Rows[0]...)
                if err != nil {
                    return err
                }
                //fmt.Printf("REPLACE INTO %s VALUES (%s)\n", name, strings.Join(cols, ","))
            }

        case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
            if len(event.Rows[0]) > 1 {
                cols := make([]string, len(event.Rows[1]))
                for i := range cols {cols[i] = "?"}

                _, err = conn.ExecContext(ctx, fmt.Sprintf("REPLACE INTO %s VALUES (%s)", name, strings.Join(cols, ",")), event.Rows[1]...)
                if err != nil {
                    return err
                }
                //fmt.Printf("REPLACE INTO %s VALUES (%s)\n", name, strings.Join(cols, ","))
            }

        case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
            table, found := tables[name]
            if found && len(event.Rows[0]) > 0 && len(table.PrimaryKeys) > 0 {
                cols := []string{}
                vals := []any{}
                for i, key := range table.PrimaryKeys {
                    cols = append(cols, fmt.Sprintf("%s = ?", i))
                    vals = append(vals, event.Rows[0][key])
                }

                _, err = conn.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s", name, strings.Join(cols, " AND ")), vals...)
                if err != nil {
                    return err
                }
            }
        }
    }

    // 4. (Опционально) Включаем лог обратно, если планируете 
    // использовать это же соединение дальше для обычных задач
    //_, err = conn.ExecContext(ctx, "SET sql_log_bin = 1")

    return nil
}