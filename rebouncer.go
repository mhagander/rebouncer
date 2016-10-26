package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-logfmt/logfmt"
	_ "github.com/lib/pq"
)

type Status int

func (status Status) String() string {
	if status == DOWN {
		return "down"
	} else if status == STANDBY {
		return "standby"
	} else if status == MASTER {
		return "master"
	}
	return "UNKNOWN"
}

const (
	DOWN Status = iota
	STANDBY
	MASTER
)

var (
	config           Config
	aggressiveChecks bool //When true, do status checks every 100ms instead of configured interval
)

func AttemptClose(db *sql.DB) {
	if db == nil {
		return
	}

	err := db.Close()
	if err != nil {
		log.Println(err)
	}
}

func decodeConnStrTokens(connStr string) (map[string]string, error) {
	decoder := logfmt.NewDecoder(strings.NewReader(connStr))
	tokens := make(map[string]string)

	decoder.ScanRecord()
	err := decoder.Err()
	if err != nil {
		return tokens, err
	}

	for decoder.ScanKeyval() {
		err = decoder.Err()
		if err != nil {
			return tokens, err
		}

		key := string(decoder.Key())
		value := string(decoder.Value())

		tokens[key] = value
	}

	return tokens, nil
}

func buildConnStr(tokens map[string]string) string {
	outStr := ""

	for key, value := range tokens {
		outStr += fmt.Sprintf("%s=%s ", key, value)
	}

	return outStr
}

//Build a connection to the pgbouncer instance that uses the credentials of the regular servers.  Goal here
//is to log into the current master via pgbouncer & make sure that everything is good
func buildPassthroughPgbouncerConnStr(pgbouncer string, serverConnStr string) (string, error) {
	pgbouncerTokens, err := decodeConnStrTokens(pgbouncer)
	if err != nil {
		return "", err
	}

	serverTokens, err := decodeConnStrTokens(serverConnStr)
	if err != nil {
		return "", err
	}

	pgbouncerTokens["user"] = serverTokens["user"]
	pgbouncerTokens["dbname"] = serverTokens["dbname"]

	password, ok := serverTokens["password"]
	if ok {
		pgbouncerTokens["password"] = password
	} else {
		delete(pgbouncerTokens, "password")
	}

	return buildConnStr(pgbouncerTokens), nil
}

func mainloop(statuschan chan []Server) {
	servers := []Server{}
	pgbouncer := &Server{name: "bouncer", connstr: config["global"]["pgbouncer"]}
	for name, connstr := range config["servers"] {
		// Make sure the file exists
		path := fmt.Sprintf("%s/%s.ini", config["global"]["configdir"], name)
		_, err := os.Stat(path)
		if err != nil {
			log.Printf("Could not load %s: %s", path, err)
			os.Exit(1)
		}
		servers = append(servers, Server{name: name, connstr: connstr})
	}
	for {
		bouncer, err := pgbouncer.OpenConnection()
		AttemptClose(bouncer)
		if err == nil {
			break
		}
		log.Printf("ERROR: could not connect to pgbouncer: %s\n", err)
		time.Sleep(5 * time.Second)
	}

	keyHealthConnStr, err := buildPassthroughPgbouncerConnStr(pgbouncer.connstr, servers[0].connstr)
	if err != nil {
		log.Fatalln(err)
	}
	pgbouncerHealth := &Server{name: "bouncerhealth", connstr: keyHealthConnStr}

	log.Printf("Connection to pgbouncer validated, starting polling")

	// Send initial status
	statuschan <- servers

	// Start a timer that will make our loop tick, and then loop
	// forever on it.
	ticker := make(chan time.Time)
	go func(ticker chan time.Time) {
		longTimer := time.Tick(time.Duration(config.getInt("global", "interval", 30)) * time.Second)
		shortTimer := time.Tick(100 * time.Millisecond)
		for {
			select {
			case out := <-shortTimer:
				if aggressiveChecks {
					ticker <- out
				}
				continue
			case out := <-longTimer:
				ticker <- out
				continue
			}
		}
	}(ticker)

	var currentmaster *Server

	for {
		// Make one poll-run across all servers in parallell, each on
		// their own goroutine. Collect and wait until all are done.
		var doneWg sync.WaitGroup
		for i := 0; i < len(servers); i++ {
			s := &servers[i]
			doneWg.Add(1)
			go func(server *Server) {
				defer doneWg.Done()
				server.CheckWithTimeout()
			}(s)
		}
		doneWg.Add(1)
		go func(server *Server) {
			defer doneWg.Done()
			server.CheckWithTimeout()
		}(pgbouncerHealth)

		doneWg.Wait()

		// Send off the newly collected status so it can be monitored
		// immediately.
		statuschan <- servers

		// Who's our new master?
		var newmaster *Server
		disable := false
		for i := 0; i < len(servers); i++ {
			s := &servers[i]
			if s.status == MASTER {
				if newmaster != nil {
					log.Printf("More than one master (at least %s and %s)! This is bad! Not touching anything!", newmaster.name, s.name)
					disable = true
				} else {
					newmaster = s
				}
			}
		}

		// We set rebouncer onto "aggressive mode" when we flip over the master so that we if there
		// are problems with the RELOAD we can hopefully spam it until it works
		// If pgbouncer is looking good we need to turn that off
		if aggressiveChecks && pgbouncerHealth.status == MASTER {
			aggressiveChecks = false
		}

		// Did the master change?
		if newmaster == nil {
			log.Printf("No master currently available! Not touching anything!")
		} else if !disable {
			// We have a master, and we've not been told to disable.
			if newmaster != currentmaster {
				if currentmaster != nil {
					log.Printf("Master changed from %s to %s", currentmaster.name, newmaster.name)
				} else {
					log.Printf("Master detected as %s", newmaster.name)
				}

				newmaster.MakeActiveMaster(pgbouncer)

				currentmaster = newmaster
			} else if pgbouncerHealth.status != MASTER {
				//We've recently flipped over the pgbouncer but it's still not pointing at a master &
				//we have a functioning master so that's weird.  Try reloading it again
				currentmaster.MakeActiveMaster(pgbouncer)
			}
		}

		// Wait for the next tick
		<-ticker
	}
}

// Commandline parameters
var configFile = flag.String("config", "rebouncer.ini", "name of configuration file")
var logFile = flag.String("logfile", "", "name of logfile")
var listenAddr = flag.String("http", "localhost:7100", "http host and port for monitoring interface")
var pidfile = flag.String("pidfile", "", "file to write pid to")

func main() {
	flag.Parse()

	config = loadConfig(*configFile)

	if *logFile != "" {
		f, err := os.OpenFile(*logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("error opening log file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	if *pidfile != "" {
		pid := syscall.Getpid()
		f, err := os.OpenFile(*pidfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("error opening pid file: %v", err)
		}
		fmt.Fprintf(f, "%d\n", pid)
		f.Close()
	}

	// Start our status collector
	statuschan := make(chan []Server)
	requestchan = make(chan chan []Server)
	go statuscollector(statuschan)

	// Something in the log to indicate we're good to go
	log.Printf("rebouncer starting up...")

	// Start our main loop
	go mainloop(statuschan)

	// Start our status http server. This will also block
	// forever.
	err := runHTTPServer()
	if err != nil {
		log.Fatalln(err)
	}
}
