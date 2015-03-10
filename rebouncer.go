package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"os"
	"strings"
	"syscall"
	"time"
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

type Server struct {
	name      string
	connstr   string
	status    Status
	lastcheck time.Time
	laststate time.Time
}

var config Config

// Check one server. Does not have timeout functionality, so the
// calling function must take care of timeouts.
func checkServer(server Server, retchan chan Status) {
	db, err := sql.Open("postgres", fmt.Sprintf("%s connect_timeout=%d", server.connstr, config.getInt("global", "timeout", 3)-1))
	if err != nil {
		retchan <- DOWN
		return
	}
	defer db.Close()
	db.SetMaxIdleConns(0)

	err = db.Ping()
	if err != nil {
		retchan <- DOWN
		return
	}

	var inrecovery bool
	err = db.QueryRow("SELECT pg_is_in_recovery()").Scan(&inrecovery)
	if err != nil {
		log.Printf("%s: query error: %s", server.name, err)
		retchan <- DOWN
		return
	}

	if inrecovery {
		retchan <- STANDBY
	} else {
		retchan <- MASTER
	}
}

// Check one server, timing out after 3 seconds or whatever is in the config.
func checkServerWithTimeout(server *Server, donechannel chan int) {
	timeout := time.After(time.Duration(config.getInt("global", "timeout", 3)) * time.Second)
	retchan := make(chan Status, 1)

	// Send the actual check
	go checkServer(*server, retchan)

	select {
	case status := <-retchan:
		if server.status != status {
			log.Printf("%s: now %v", server.name, status)
			server.status = status
			server.laststate = time.Now()
		}
	case <-timeout:
		// Something timed out, so we're going to ignore the
		// result and set this node as down.
		if server.status != DOWN {
			log.Printf("%s: timeout", server.name)
			server.status = DOWN
			server.laststate = time.Now()
		}
	}
	server.lastcheck = time.Now()
	donechannel <- 1
}

// Return a validated connection to pgbouncer. If no connection
// can be made, logs the error and returns nil.
func getValidBouncerConnection() *sql.DB {
	bouncer, err := sql.Open("postgres", config["global"]["pgbouncer"])
	if err != nil {
		log.Printf("ERROR: could not connect to pgbouncer: %s", err)
		return nil
	}
	bouncer.SetMaxIdleConns(0)

	err = bouncer.Ping()
	if err != nil {
		log.Printf("ERROR: could not connect to pgbouncer: %s", err)
		return nil
	}

	return bouncer
}

// Actually reconfigure pgbouncer
func flipActiveMaster(server *Server) {
	// First connect to pgbouncer to make sure we can
	bouncer := getValidBouncerConnection()
	if bouncer == nil {
		// Error already logged
		return
	}
	defer bouncer.Close()

	// Then flip the actual symlink
	err := os.Remove(config["global"]["symlink"])
	if err != nil {
		log.Printf("ERROR: failed to remove old symlink: %s", err)
		return
	}

	err = os.Symlink(fmt.Sprintf("%s/%s.ini", strings.TrimRight(config["global"]["configdir"], "/"), server.name), config["global"]["symlink"])
	if err != nil {
		log.Printf("ERROR: failed to set symlink for server %s: %s", server.name, err)
		return
	}

	_, err = bouncer.Exec("RELOAD")
	if err != nil {
		log.Printf("ERROR: failed to reload pgbouncer: %s", err)
		return
	}

	log.Printf("pgbouncer reconfigured for new master %s", server.name)
}

func mainloop(statuschan chan []Server) {
	servers := []Server{}
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
		bouncer := getValidBouncerConnection()
		if bouncer == nil {
			// Error already logged
			time.Sleep(5 * time.Second)
		} else {
			bouncer.Close()
			break
		}
	}

	log.Printf("Connection to pgbouncer validated, starting polling")

	// Send initial status
	statuschan <- servers

	// Start a timer that will make our loop tick, and then loop
	// forever on it.
	ticker := time.Tick(time.Duration(config.getInt("global", "interval", 30)) * time.Second)

	var currentmaster *Server = nil

	for {
		// Make one poll-run across all servers in parallell, each on
		// their own goroutine. Collect and wait until all are done.
		donechannel := make(chan int, len(servers))
		for i := 0; i < len(servers); i++ {
			s := &servers[i]
			go checkServerWithTimeout(s, donechannel)
		}
		for _ = range servers {
			<-donechannel
		}

		// Send off the newly collected status so it can be monitored
		// immediately.
		statuschan <- servers

		// Who's our new master?
		var newmaster *Server = nil
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

				flipActiveMaster(newmaster)

				currentmaster = newmaster
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
	runHttpServer()
}
