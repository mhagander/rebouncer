package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

type Server struct {
	name      string
	connstr   string
	status    Status
	lastcheck time.Time
	laststate time.Time
}

func (server *Server) OpenConnection() (*sql.DB, error) {
	db, err := sql.Open("postgres", fmt.Sprintf("%s connect_timeout=%d", server.connstr, config.getInt("global", "timeout", 3)-1))
	if err != nil {
		return db, err
	}

	db.SetMaxIdleConns(0)

	err = db.Ping()
	return db, err
}

// Check one server. Does not have timeout functionality, so the
// calling function must take care of timeouts.
func (server *Server) Check(retchan chan Status) {
	db, err := server.OpenConnection()
	defer AttemptClose(db)
	if err != nil {
		log.Printf("%s: failed to open connection: %s", server.name, err)
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
func (server *Server) CheckWithTimeout() {
	timeout := time.After(time.Duration(config.getInt("global", "timeout", 3)) * time.Second)
	retchan := make(chan Status, 1)

	// Send the actual check
	go server.Check(retchan)

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
}

// Actually reconfigure pgbouncer
func (server *Server) MakeActiveMaster(pgbouncer *Server) {
	// First connect to pgbouncer to make sure we can
	bouncer, err := pgbouncer.OpenConnection()
	defer AttemptClose(bouncer)
	if err != nil {
		log.Printf("PGBOUNCER: Could not connect - %s", err)
		return
	}

	// Then flip the actual symlink
	err = os.Remove(config["global"]["symlink"])
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

	//Now that we've tried to turn over the master, we need to very rapidly determine if it worked and if not, RELOAD again.
	aggressiveChecks = true

	log.Printf("pgbouncer reconfigured for new master %s", server.name)
}
