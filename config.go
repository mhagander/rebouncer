package main

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"
)

type section map[string]string
type Config map[string]section

func (c Config) getInt(section string, key string, defaultval int64) int64 {
	val, ok := c[section][key]
	if ok {
		i, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return defaultval
		} else {
			return i
		}
	} else {
		return defaultval
	}
}

func loadConfig(filename string) Config {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Could not open configuration file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	cfg := make(Config)
	var currsection section
	var currsectionname string
	currsection = nil
	currsectionname = ""

	linenum := 0
	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())
		linenum++

		if text == "" || strings.HasPrefix(text, ";") || strings.HasPrefix(text, "#") {
			continue
		}
		if strings.HasPrefix(text, "[") && strings.HasSuffix(text, "]") {
			// This is a new section
			if currsectionname != "" {
				cfg[currsectionname] = currsection
			}
			currsection = make(section)
			currsectionname = strings.TrimRight(strings.TrimLeft(text, "["), "]")
		} else if !strings.Contains(text, "=") {
			log.Fatalf("Missing = sign on line %d (%s)", linenum, text)
		} else {
			if currsectionname == "" {
				log.Fatal("Config value without section!")
			}

			s := strings.SplitN(text, "=", 2)
			currsection[s[0]] = s[1]
		}
	}
	if currsectionname != "" {
		cfg[currsectionname] = currsection
	}

	return cfg
}
