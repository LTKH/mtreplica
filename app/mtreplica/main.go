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
    chanEventsCnt = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "mtreplica_chan_events_count",
            Help: "",
        },
        []string{"client", "name"},
    )
)

type Replica struct {
    Events    chan config.Event
}

type InfoSchema struct {
    sync.RWMutex
    tables  map[string]int64
}

func NewInfoSchema() *InfoSchema {
    infoSchema := InfoSchema{
        tables: make(map[string]int64),
    }
    return &infoSchema
}

func (i *InfoSchema) Set(name string, timestamp int64) {
    i.Lock()
    defer i.Unlock()
    i.tables[name] = timestamp
}

func (i *InfoSchema) Get(name string) (int64) {
    i.RLock()
    defer i.RUnlock()
    timestamp, found := i.tables[name]
    if !found {
        return 0
    }
    return timestamp
}

func main() {

    // Command-line flag parsing
    lsAddress := flag.String("listen.client-address", "127.0.0.1:8428", "listen address")
    cfFile    := flag.String("config.file", "config/mtreplica.yml", "config file")
    flag.Parse()

    // Program signal processing
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
    go func(){
        for {
            s := <-c
            switch s {
                case syscall.SIGINT:
                    log.Print("INFO mtreplica stopped")
                    os.Exit(0)
                case syscall.SIGTERM:
                    log.Print("INFO mtreplica stopped")
                    os.Exit(0)
                default:
                    log.Print("INFO unknown signal received")
            }
        }
    }()

    prometheus.MustRegister(logEventsTotal)
    prometheus.MustRegister(chanEventsCnt)

    go func(){
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

    slave, err := db.NewClient(cfg.SlaveNode)
    if err != nil {
        log.Fatalf("ERROR connecting to slave (%s): %v", cfg.SlaveNode.Flavor, err)
    }

    go func(){
        for {
            var events []config.Event

            for i := 0; i < len(replica.Events); i++ {
                event := <-replica.Events
                events = append(events, event)
            }

            if len(events) > 0 {
                err := slave.SetEvents(events)
                if err != nil {
                    log.Printf("ERROR %v", err)
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

            master, err := db.NewClient(n)
            if err != nil {
                log.Fatalf("ERROR connecting to master (%s): %v", n.Flavor, err)
            }

            tables, err := master.GetTables()
            if err != nil {
                log.Fatalf("ERROR getting list of tables: %v", err)
            }
            for _, t := range tables {
                infoSchema.Set(fmt.Sprintf("%s.%s", t.Schema, t.Table), t.Timestamp)
            }

            syncer := replication.NewBinlogSyncer(node)

            // Start sync with specified binlog file and position
            streamer, _ := syncer.StartSync(mysql.Position{})
            schema, table := "", ""

            for {
                // 3. Получение события
                ev, err := streamer.GetEvent(context.Background())
                if err != nil {
                    log.Printf("ERROR getting event: %v", err)
                    break
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

                if infoSchema.Get(fmt.Sprintf("%s.%s", schema, table)) <= int64(ev.Header.Timestamp) {
                    // 4. Обработка события
                    // Например, выведем тип события и время
                    //log.Printf("INFO event type: %s, timestamp: %d\n", ev.Header.EventType, ev.Header.Timestamp)
                    //log.Printf("ROWS %v", rows)

                    select {
                    case replica.Events <- config.Event{Schema: schema, Table: table, Type: ev.Header.EventType, Rows: rows, Timestamp: ev.Header.Timestamp}:
                    default:
                        // Error stat
                    }
                }

            }

        }(n)
    }

    // Daemon mode
    for {
        chanEventsCnt.With(prometheus.Labels{"client": cfg.SlaveNode.Flavor, "name": cfg.SlaveNode.Name}).Set(float64(len(replica.Events)))
        time.Sleep(10 * time.Second)
    }
}