package main

import (
    "log"
    "fmt"
    "flag"
    "time"
    "context"
    "net/http"
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
    eventsTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name:      "mtreplica_events_total",
            Help:      "",
        },
        []string{"server_id","schema","table","type"},
    )
)

func main() {

    // Command-line flag parsing
    lsAddress := flag.String("listen.client-address", "127.0.0.1:8427", "listen address")
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

    prometheus.MustRegister(eventsTotal)

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

    for _, n := range cfg.MasterNodes {

        go func(n config.MasterNode){

            // Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
            // flavor is mysql or mariadb
            node := replication.BinlogSyncerConfig {
                ServerID: n.ServerID,
                Flavor:   n.Flavor,
                Host:     n.Host,
                Port:     n.Port,
                User:     n.User,
                Password: n.Password,
            }

            _, err := db.NewClient(n)
            if err != nil {
                log.Fatalf("ERROR %v", err)
            }

            syncer := replication.NewBinlogSyncer(node)

            // Start sync with specified binlog file and position
            streamer, _ := syncer.StartSync(mysql.Position{})
            schema, table := []byte(""), []byte("")

            for {
                // 3. Получение события
                ev, err := streamer.GetEvent(context.Background())
                if err != nil {
                    log.Printf("ERROR getting event: %v", err)
                    break
                }

                switch e := ev.Event.(type) {
                // 2. Сначала ловим TableMapEvent и сохраняем его
                case *replication.TableMapEvent:
                    schema, table = e.Schema, e.Table
                case *replication.RowsEvent:
                    //fmt.Printf("Data: %v\n", e.Rows)
                }

                switch ev.Header.EventType {
                case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
                    eventsTotal.With(prometheus.Labels{"server_id": fmt.Sprintf("%v", n.ServerID), "schema": string(schema), "table": string(table), "type": "insert"}).Inc()
                
                case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
                    eventsTotal.With(prometheus.Labels{"server_id": fmt.Sprintf("%v", n.ServerID),"schema": string(schema), "table": string(table), "type": "update"}).Inc()
                
                case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
                    eventsTotal.With(prometheus.Labels{"server_id": fmt.Sprintf("%v", n.ServerID), "schema": string(schema), "table": string(table), "type": "delete"}).Inc()
                }

                // 4. Обработка события
                // Например, выведем тип события и время
                log.Printf("INFO event type: %s, timestamp: %d\n", ev.Header.EventType, ev.Header.Timestamp)

            }

        }(n)
    }

    // Daemon mode
    for {
        time.Sleep(600 * time.Second)
    }
}