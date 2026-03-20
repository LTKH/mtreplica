package main

import (
    "log"
    "log/slog"
    "fmt"
    "flag"
    "time"
    "sync"
    "context"
    "net/http"
    "io"
    "os"
    "os/signal"
    "syscall"
    "github.com/go-mysql-org/go-mysql/mysql"
    "github.com/go-mysql-org/go-mysql/replication"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"

    "github.com/ltkh/mtreplica/internal/db"
    "github.com/ltkh/mtreplica/internal/config"
)

var (
    logEventsTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "mtreplica_log_events_total",
            Help: "",
        },
        []string{"client", "server_id", "schema", "table", "type"},
    )
    chanEventsDropped = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "mtreplica_chan_events_dropped",
            Help: "",
        },
        []string{"client", "schema", "table", "type"},
    )
    chanEventsCnt = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "mtreplica_chan_events_count",
            Help: "",
        },
        []string{"client", "name"},
    )
)

type Replica struct {
    Events      chan config.Event
}

type InfoSchema struct {
    sync.RWMutex
    Tables      map[string]config.Table
}

func NewInfoSchema() *InfoSchema {
    infoSchema := InfoSchema{
        Tables: make(map[string]config.Table),
    }
    return &infoSchema
}

func (i *InfoSchema) Set(name string, table config.Table) {
    i.Lock()
    defer i.Unlock()
    i.Tables[name] = table
}

func (i *InfoSchema) Get(name string) (config.Table) {
    i.RLock()
    defer i.RUnlock()
    table, found := i.Tables[name]
    if !found {
        return config.Table{}
    }
    return table
}

func (i *InfoSchema) GetAll() (map[string]config.Table) {
    i.RLock()
    defer i.RUnlock()
    return i.Tables
}

func TableSynch(master, slave db.DbClient, tables map[string]config.Table) (error) {
    return nil
}

func main() {

    // Command-line flag parsing
    lsAddress := flag.String("listen.client-address", "127.0.0.1:8428", "listen address")
    cfFile    := flag.String("config.file", "config/mtreplica.yml", "config file")
    debug     := flag.Bool("debug", false, "debug mode")
    flag.Parse()

    // Program completion signal processing
    c := make(chan os.Signal, 2)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)
    go func() {
        <-c
        log.Print("INFO mtreplica stopped")
        os.Exit(0)
    }()

    // Enabling monitoring data
    go func(){
        prometheus.MustRegister(logEventsTotal)
        prometheus.MustRegister(chanEventsDropped)
        prometheus.MustRegister(chanEventsCnt)

        mux := http.NewServeMux()
        mux.Handle("/metrics", promhttp.Handler())
        if err := http.ListenAndServe(*lsAddress, mux); err != nil {
            log.Fatalf("ERROR %v", err)
        }
    }()

    // Loading configuration file
    cfg, err := config.NewConfig(*cfFile)
    if err != nil {
        log.Fatalf("ERROR %v", err)
    }

    infoSchema := NewInfoSchema()

    replica := &Replica{
        Events: make(chan config.Event, 1000000),
    }

    if cfg.SlaveNode.Name == "" {
        log.Fatalf("ERROR empty DB name for slave")
    }

    slave, err := db.NewClient(cfg.SlaveNode)
    if err != nil {
        log.Fatalf("ERROR connecting to slave: %v", err)
    }

    tables, err := slave.GetTablesInfo(cfg.SlaveNode.Name)
    if err != nil {
        log.Fatalf("ERROR getting list of tables: %v", err)
    }
    
    for name, table := range tables {
        infoSchema.Set(name, table)
        if *debug {
            log.Printf("DEBUG %s: %v", name, table)
        }
    }

    go func(){
        for {
            var events []config.Event

            for i := 0; i < len(replica.Events); i++ {
                event := <-replica.Events
                events = append(events, event)
            }

            if len(events) > 0 {
                err := slave.SetEvents(infoSchema.GetAll(), events)
                if err != nil {
                    log.Printf("ERROR events saving: %v", err)
                }
            }
        }
    }()

    for _, n := range cfg.MasterNodes {

        go func(n config.Node){

            // Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
            // flavor is mysql or mariadb
            node := replication.BinlogSyncerConfig {
                ServerID: n.ServerID,
                Flavor:   n.Flavor,
                Host:     n.Host,
                Port:     n.Port,
                User:     n.User,
                Password: n.Password,
                Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
            }

            syncer := replication.NewBinlogSyncer(node)

            master, err := db.NewClient(n)
            if err != nil {
                log.Fatalf("ERROR connecting to master (%s): %v", n.Flavor, err)
            }
            
            go func(){
                for {
                    err := TableSynch(master, slave, tables)
                    if err != nil {
                        log.Printf("ERROR table synchronization: %v", err)
                    }
                    time.Sleep(10 * time.Second)
                }
            }()

            // Start sync with specified binlog file and position
            streamer, _ := syncer.StartSync(mysql.Position{})
            schema, table := "", ""

            for {
                // 3. Получение события
                ev, err := streamer.GetEvent(context.Background())
                if err != nil {
                    log.Printf("ERROR getting event: %v", err)
                    time.Sleep(10 * time.Second)
                    continue
                }

                rows := [][]any{}

                switch e := ev.Event.(type) {
                // 2. Сначала ловим TableMapEvent и сохраняем его
                case *replication.TableMapEvent:
                    schema, table = string(e.Schema), string(e.Table)
                case *replication.RowsEvent:
                    rows = e.Rows
                }

                if schema != "" && table != "" {
                    logEventsTotal.With(prometheus.Labels{"client": n.Flavor, "server_id": fmt.Sprintf("%v", n.ServerID), "schema": schema, "table": table, "type": fmt.Sprintf("%s", ev.Header.EventType)}).Inc()
                }

                if n.Name != "" && n.Name != schema {
                    continue
                }

                if infoSchema.Get(fmt.Sprintf("%s.%s", schema, table)).Timestamp <= int64(ev.Header.Timestamp) {
                    select {
                    case replica.Events <- config.Event{Schema: schema, Table: table, Type: ev.Header.EventType, Rows: rows, Timestamp: ev.Header.Timestamp}:
                    default:
                        chanEventsDropped.With(prometheus.Labels{"client": cfg.SlaveNode.Flavor, "schema": schema, "table": table, "type": fmt.Sprintf("%s", ev.Header.EventType)}).Inc()
                    }
                }
            }
        }(n)
    }

    log.Print("INFO mtreplica started")

    // Daemon mode
    for {
        chanEventsCnt.With(prometheus.Labels{"client": cfg.SlaveNode.Flavor, "name": cfg.SlaveNode.Name}).Set(float64(len(replica.Events)))
        time.Sleep(10 * time.Second)
    }
}