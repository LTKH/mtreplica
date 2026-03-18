package config

import (
    "io/ioutil"
    "gopkg.in/yaml.v2"
)

type Config struct {
    MasterNodes      []MasterNode      `yaml:"master_nodes"`
}

type MasterNode struct {
    Flavor           string            `yaml:"client"`
    ServerID         uint32            `yaml:"server_id"`
    Host             string            `yaml:"host"`
    Port             uint16            `yaml:"port"`
    User             string            `yaml:"user"`
    Password         string            `yaml:"password"`
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