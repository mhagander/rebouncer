package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"
)

// Global channel to talk to the status collector
var requestchan chan chan []Server

// Constantly running goroutine that handles passing of status
// messages. Accepts new statuses from the running checks, and
// dispatches it to any status reporting goroutines.
func statuscollector(statuschan chan []Server) {
	status := []Server{}
	for {
		select {
		case newstatus := <-statuschan:
			status = newstatus
		case req := <-requestchan:
			req <- status
		}
	}
}

// Return an array with all server statuses, by fetcing from
// the status collector.
func getServerStatus() []Server {
	c := make(chan []Server, 1)
	requestchan <- c
	return <-c
}

//-----------
// http views
//-----------
func httpRootHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Current time: %s\n", time.Now().Local())
	fmt.Fprintf(w, "Active goroutines: %d\n", runtime.NumGoroutine())
	fmt.Fprintf(w, "\n\nNode status:\n")

	servers := getServerStatus()

	for _, s := range servers {
		fmt.Fprintf(w, "%s: %s (last checked %s)\n", s.name, s.status, s.lastcheck)
	}
}

func httpNodesHandler(w http.ResponseWriter, r *http.Request) {
	servers := getServerStatus()

	for _, s := range servers {
		fmt.Fprintf(w, "%s: %s\n", s.name, s.status)
	}
}

func httpNagiosHandler(w http.ResponseWriter, r *http.Request) {
	mastercount := 0
	standbycount := 0
	downcount := 0
	oldestcheck := time.Now()

	servers := getServerStatus()

	for _, s := range servers {
		if s.status == MASTER {
			mastercount++
		} else if s.status == STANDBY {
			standbycount++
		} else {
			downcount++
		}
		if oldestcheck.After(s.lastcheck) {
			oldestcheck = s.lastcheck
		}
	}

	secondssincelast := int64(time.Now().Sub(oldestcheck).Seconds())
	maxage := config.getInt("global", "interval", 30) * 3

	if mastercount == 0 {
		fmt.Fprintf(w, "CRITICAL: No master available (%d standbys, %d down)", standbycount, downcount)
	} else if mastercount > 1 {
		fmt.Fprintf(w, "CRITICAL: Multiple masters available! Split brain waning! (%d masters, %d standbys, %d down)", mastercount, standbycount, downcount)
	} else if downcount > 0 {
		fmt.Fprintf(w, "WARNING: %d servers down (%d master, %d standbys active)", downcount, mastercount, standbycount)
	} else if secondssincelast > maxage {
		fmt.Fprintf(w, "WARNING: oldest check %d seconds ago, more than %d", secondssincelast, maxage)
	} else {
		fmt.Fprintf(w, "OK: %d masters, %d standbys active", mastercount, standbycount)
	}
}

func runHTTPServer() error {
	http.HandleFunc("/", httpRootHandler)
	http.HandleFunc("/nodes", httpNodesHandler)
	http.HandleFunc("/nagios", httpNagiosHandler)
	log.Printf("Starting status http listener at http://%s", *listenAddr)
	return http.ListenAndServe(*listenAddr, nil)
}
