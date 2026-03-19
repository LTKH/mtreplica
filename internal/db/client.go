package db

import (
    "errors"
    "github.com/ltkh/mtreplica/internal/config"
    "github.com/ltkh/mtreplica/internal/db/mysql"
)

type DbClient interface {
    GetTables() ([]mysql.Table, error)
    SetEvents(events []config.Event) (error)
    
    /*
    
    LoadTables() error
    Close() error

    SaveStatus(rec config.SockTable) error
    SaveNetstat(rec config.SockTable) error
    SaveTracert(rec config.SockTable) error

    LoadRecords(args config.RecArgs) ([]config.SockTable, error)
    SaveRecord(rec config.SockTable) error
    DelRecord(id string) error

    LoadExceptions(args config.ExpArgs) ([]config.Exception, error)
    SaveException(rec config.SockTable) error
    DelException(id string) error
    */
}

func NewClient(n config.Node) (DbClient, error) {
    switch n.Flavor {
    case "mysql":
        return mysql.New(n)
    }
    return nil, errors.New("invalid client")
}