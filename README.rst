=========
rebouncer
=========

`Rebouncer` is a simple tool to control a local instance of pgbouncer, to
make sure it always points to the master server in a failover cluster. It
is intended for the scenario where `pgbouncer` runs on every application
server node in a cluster, connecting to a central cluster of PostgreSQL
servers. It is intended for the scenario where only the master server
is interesting, and does not handle things like load balancing or
distribution of read queries.

`rebouncer` does not in any way control actual replication or database
server failover. This task is left to a separate tool, such as for
example `repmgr`.

The tool will continuously poll all defined servers, giving each node one
of three states - `master`, `standby` or `down`. If the node that is
currently `master` changes, it will reconfigure `pgbouncer` to use
this node instead of the current one.

If more than one master exists (split brain!), `rebouncer` will refuse
to make any changes to the current configuration, so as to limit the
effects of a split brain. This should mean that if an old server
reappears after all nodes have failed over, it will not be used. There
is of course the risk that some application server would not know
about this state and use it anyway, or a client that is not behind
a managed `pgbouncer`. For this reason, it is still important to properly
fence the servers in the replication cluster, to make sure this can
not happen.

Actual failover of the `pgbouncer` configuration is handled by replacing
a symlink. The active configuration will at all times be symlinked to the
currently active node configuration. This is done so that as little as
possible logic will execute when a failover happens - so instead of
trying to edit a configuration file in place, `rebouncer` requires that
all configuration files are "pre-cooked" and ready in a directory. When
the symlink has changed, `rebouncer` will connect to `pgbouncer` and issue
a `RELOAD` command.

Running
-------
`rebouncer` is run as a regular commandline, but would normally be started
from an init script or similar. An example initscript for RedHat Enterprise
is included. When run, `rebouncer` takes the following commandline
parameters, all optional:

-config
  Specifies the name of the configuration file to use. If not specified,
  the file `rebouncer.ini` in the current working directory will be used.
-logfile
  Specifies the name of a file to write the log to. If not specified, the
  the log will be sent to stdout.
-pidfile
  Specifies the name of a file to write the process identifier to after
  startup. If not specified, no process identifier will be written.
-http
  Specifies the listener interface for the status information web server,
  in the format `<address>:<port>`. If not specified, the listener will
  bind to `localhost:7100` which is only accessible from the local machine.


Configuration file
------------------
The configuration file for `rebouncer` is an INI-style plaintext file.
It contains two sections, `global` and `servers`. The `global` section
contains the following settings controlling the global behavior of
`rebouncer`:

pgbouncer
  A lib/pq style connection string for connecting to pgbouncer. If
  `rebouncer` cannot connect to `pgbouncer` at startup, it will fail
  to start.
symlink
  The full path of the symbolic link to reconfigure on failover. This
  must be in a directory where `rebouncer` has permissions to remove
  the old symlink and create a new one.
configdir
  The full path of the directory containing the node specific
  configuration file. In this directory there should be one file for
  each node, named `<nodename>.ini`, each being a complete
  `pgbouncer` configuration file.
interval
  Number of seconds between polling servers. All servers are polled
  in parallel once this timer has expired. If not specified, 30 seconds
  is used as interval.

The `servers` section has one setting for each server that is a member
of the cluster. The settings name is the name of the server as being
used internally in `rebouncer`, and must match the name of the file in
the `<configdir>` directory. The value of the setting is a lib/pq
style connection string for connecting to this server.

Connection strings
------------------
As `rebouncer` is written in `go`, it uses the `lib/pq` driver to access
both `pgbouncer` and the servers. This means that connection strings have
to be compatible with `lib/pq`. These are compatible with standard
PostgreSQL connection strings, with one important exception - the
value for `sslmode`. `lib/pq` does not support the negotiation modes, so
only these settings are possible:

* disable
* require (*default*)
* verify-ca
* verify-full

Note that `require` is the default value, which means that unless you
explicitly specify `sslmode=disable` in your connection string, it will
not be possible to connect to `pgbouncer`. For simplicity (in order to
avoid having to parse the connection string), `rebouncer` leaves it up
to the configuration file to specify this correctly.

Status webserver
----------------
A small webserver runs that can be used to view the current status of
the `rebouncer` instance. It serves a few endpoints:

/
  A generic status overview
/nodes
  A list of which nodes have which status, for parsing (the root URL
  gives a more detailed status).
/nagios
  A nagios compatible output for attaching a monitor to
/debug/pprof/
  The `go` default debug view, which shows details about what different
  goroutines are currently up to, including stack traces.

This webserver is not protected in any way, so normally it needs to be
protected either by binding only to a localhost interface, or by using
kernel firewall rules.

Nagios integration
------------------
The output of the webservers `/nagios` URL gives an example URL for using
with Nagios monitors. An example plugin is also included in the
`/nagios` directory, which needs to be fed the base URL of the `rebouncer`
webserver to work.
