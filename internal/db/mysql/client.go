package mysql

import (
    //"os"
    "fmt"
    //"log"
    //"sync"
    //"time"
    //"errors"
    //"encoding/json"
    "database/sql"
    "github.com/go-sql-driver/mysql"
    "github.com/ltkh/mtreplica/internal/config"
)

type Client struct {
    client     *sql.DB
}

func New(n config.MasterNode) (*Client, error) {

    cfg := mysql.Config{
        User:   n.User,
        Passwd: n.Password,
        Net:    "tcp",
        Addr:   fmt.Sprintf("%s:%s", n.Host, n.Port), // Хост и порт здесь
        //DBName: "testdb",
    }

    conn, err := sql.Open("mysql", cfg.FormatDSN())
    if err != nil {
        return nil, err
    }

    return &Client{client: conn}, nil
}

func (db *Client) CreateTables() error {
    _, err := db.client.Exec(fmt.Sprintf(`
      create table if not exists history (
        %[1]stimestamp%[1]s     bigint(20) primary key,
        %[1]sweather%[1]s       json,
        %[1]ssensors%[1]s       json,
        %[1]sreaction%[1]s      json,
        unique key IDX_timestamp (timestamp)
      ) engine InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;
    `, "`"))
    if err != nil {
        return err
    }
    return nil
}