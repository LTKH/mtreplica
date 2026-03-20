package config

import (
    "io/ioutil"
    "gopkg.in/yaml.v2"
    "github.com/go-mysql-org/go-mysql/replication"
)

type Config struct {
    MasterNodes      []Node            `yaml:"master_nodes"`
    SlaveNode        Node              `yaml:"slave_node"`
}

type Node struct {
    Flavor           string            `yaml:"client"`
    ServerID         uint32            `yaml:"server_id"`
    Host             string            `yaml:"host"`
    Port             uint16            `yaml:"port"`
    User             string            `yaml:"user"`
    Password         string            `yaml:"password"`
    Name             string            `yaml:"name"`
}

type Event struct {
    Schema    string
    Table     string
    Type      replication.EventType
    Rows      [][]any
    Timestamp uint32
}

type Table struct {
    Timestamp   int64
    PrimaryKeys map[string]int
}

func NewConfig(filename string) (*Config, error) {

    cfg := &Config{}

    content, err := ioutil.ReadFile(filename)
    if err != nil {
       return cfg, err
    }

    if err := yaml.UnmarshalStrict(content, cfg); err != nil {
        return cfg, err
    }
    
    return cfg, nil
}