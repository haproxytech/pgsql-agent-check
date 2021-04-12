/*
-- HAProxy agent check for PostgreSQL
--
-- The check detects a Master and reports the node as available, otherwise it reports the node as DOWN.
--
-- Copyright (c) 2021. Michel Mayen <mmayen@haproxy.com>
-- Copyright (c) 2021. HAProxy Technologies, LLC.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
-- SPDX-License-Identifier: Apache-2.0
*/
package main

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/Kount/pq-timeouts"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const DEFAULT_ADDRESS string = "127.0.0.1"
const DEFAULT_PORT string = "5432"
const DEFAULT_LOG_FILE string = "pgsql-agent-check.log"
const DEFAULT_WORKER string = "2"
const DEFAULT_DELIMITER string = "|"
const DEFAULT_LOG_LEVEL string = "1"
const DEFAULT_PG_TIMEOUT string = "1000"
const DEFAULT_RUN_IN_DOCKER string = "no"
const LOG_PROCESS_EVENT uint8 = 1
const LOG_NETWORK_ERR uint8 = 2
const LOG_CONNECT_ACCEPT uint8 = 4
const LOG_CHECK_EVENT uint8 = 8
const LOG_CHECK_REQ uint8 = 16
const LOG_CHECK_DEBUG uint8 = 32
const LOG_DB_ERR uint8 = 64

const HELP string = `Help :
--address        The agent listens on this address (default %s)
--port           The agent listens on this port (default %s)
--pg-timeout     The timeout applied on the pg connections (default %s ms)
--worker         The agent runs N worker (default %s)
--run-in-docker  Set to yes when it is run in docker (default %s)
--logfile        The agent writes its log to this file (default %s)
--loglevel       The log verbosity level (default %s)
`

type PgsqlConnectionParameters struct {
	Host     string
	Port     int
	Username string
	Password string
	DbName   string
	Timeout  string
	logChan  chan *LogMessage
	dbConn   *sql.DB
	reason   string
}

func (pcp *PgsqlConnectionParameters) String() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s read_timeout=%s write_timeout=%s sslmode=disable", pcp.Host, pcp.Port, pcp.Username, pcp.Password, pcp.DbName, pcp.Timeout, pcp.Timeout)
}

type LogMessage struct {
	Message  error
	LogLevel uint8
}

func (lm *LogMessage) Error() string {
	return lm.Message.Error()
}
func NewLogMessage(err error, logLevel uint8) *LogMessage {
	var lm *LogMessage = new(LogMessage)
	lm.Message = err
	lm.LogLevel = logLevel
	return lm
}

type AgentParameters struct {
	address        string
	port           string
	pgTimeout      string
	logFile        string
	logLevel       string
	worker         string
	runInDocker    string
	logChan        chan *LogMessage
	wgWorker       *sync.WaitGroup
	wgServer       *sync.WaitGroup
	wgLogger       *sync.WaitGroup
	wgKiller       *sync.WaitGroup
	stopLoggerChan chan bool
	stopWorkerChan chan bool
	connChan       chan *net.TCPConn
	listener       *net.TCPListener
}

func (ap *AgentParameters) GetTCPAddr() *net.TCPAddr {
	var port int
	var address net.IP
	var err error
	if err = address.UnmarshalText([]byte(ap.address)); err != nil {
		ap.logChan <- NewLogMessage(err, LOG_NETWORK_ERR)
		ap.logChan <- NewLogMessage(errors.New("Fallback to the default address ("+DEFAULT_ADDRESS+")."), LOG_NETWORK_ERR)
		_ = address.UnmarshalText([]byte(DEFAULT_ADDRESS))
	}
	if port, err = strconv.Atoi(ap.port); err != nil {
		ap.logChan <- NewLogMessage(err, LOG_NETWORK_ERR)
		ap.logChan <- NewLogMessage(errors.New("Fallback to the default address ("+DEFAULT_PORT+")."), LOG_NETWORK_ERR)
		port, _ = strconv.Atoi(DEFAULT_PORT)
	}
	return &net.TCPAddr{
		IP:   address,
		Port: port,
	}
}
func (ap *AgentParameters) GetNbWorker() int {
	var nbWorker int
	var err error
	if nbWorker, err = strconv.Atoi(ap.worker); err != nil {
		ap.logChan <- NewLogMessage(err, LOG_PROCESS_EVENT)
		ap.logChan <- NewLogMessage(errors.New("Fallback to the default number of worker("+DEFAULT_WORKER+")."), LOG_PROCESS_EVENT)
		nbWorker, _ = strconv.Atoi(DEFAULT_WORKER)
	}
	return nbWorker
}
func NewAgentParameters() *AgentParameters {
	var ap *AgentParameters = new(AgentParameters)
	ap.address = DEFAULT_ADDRESS
	ap.port = DEFAULT_PORT
	ap.pgTimeout = DEFAULT_PG_TIMEOUT
	ap.runInDocker = DEFAULT_RUN_IN_DOCKER
	ap.logFile = DEFAULT_LOG_FILE
	ap.logLevel = DEFAULT_LOG_LEVEL
	ap.worker = DEFAULT_WORKER
	ap.logChan = make(chan *LogMessage)
	ap.wgWorker = new(sync.WaitGroup)
	ap.wgServer = new(sync.WaitGroup)
	ap.wgLogger = new(sync.WaitGroup)
	ap.wgKiller = new(sync.WaitGroup)
	ap.stopLoggerChan = make(chan bool)
	ap.stopWorkerChan = make(chan bool)
	ap.connChan = make(chan *net.TCPConn)
	return ap
}

func main() {
	var sigs chan os.Signal = make(chan os.Signal, 1)
	var agentParameters *AgentParameters = ParseArgs()
	agentParameters.wgLogger.Add(1)
	go Logger(agentParameters)
	for i := 0; i < agentParameters.GetNbWorker(); i++ {
		agentParameters.wgWorker.Add(1)
		go Worker(agentParameters, i)
	}
	agentParameters.wgServer.Add(1)
	go Server(agentParameters)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGUSR1)
	var sig os.Signal = <-sigs
	switch sig {
	case syscall.SIGUSR1:
		agentParameters.logChan <- NewLogMessage(errors.New("HAProxy reloads."), LOG_PROCESS_EVENT)
		agentParameters.listener.Close()
		agentParameters.wgKiller.Add(1)
		StopAllRoutines(agentParameters)
	case syscall.SIGTERM:
		agentParameters.logChan <- NewLogMessage(errors.New("HAProxy stops/restarts."), LOG_PROCESS_EVENT)
		agentParameters.listener.Close()
		agentParameters.wgKiller.Add(1)
		StopAllRoutines(agentParameters)
	default:
		agentParameters.logChan <- NewLogMessage(errors.New(fmt.Sprintf("Received %s", fmt.Sprint(sig))), LOG_PROCESS_EVENT)
		fmt.Println(sig)
		agentParameters.listener.Close()
		agentParameters.wgKiller.Add(1)
		StopAllRoutines(agentParameters)
	}
	agentParameters.wgKiller.Wait()
}

func ParseArgs() *AgentParameters {
	var agentParameters *AgentParameters = NewAgentParameters()
	var setting *string
	var waitValue bool
	for _, arg := range os.Args {
		if waitValue {
			waitValue = false
			*setting = arg
		} else {
			switch arg {
			case "--address":
				setting = &agentParameters.address
				waitValue = true
			case "--port":
				setting = &agentParameters.port
				waitValue = true
			case "--worker":
				setting = &agentParameters.worker
				waitValue = true
			case "--logfile":
				setting = &agentParameters.logFile
				waitValue = true
			case "--loglevel":
				setting = &agentParameters.logLevel
				waitValue = true
			case "--pg-timeout":
				setting = &agentParameters.pgTimeout
				waitValue = true
			case "--run-in-docker":
				setting = &agentParameters.runInDocker
				waitValue = true
			case "--help":
				fmt.Printf(HELP, DEFAULT_ADDRESS, DEFAULT_PORT, DEFAULT_PG_TIMEOUT, DEFAULT_WORKER, DEFAULT_RUN_IN_DOCKER, DEFAULT_LOG_FILE, DEFAULT_LOG_LEVEL)
				os.Exit(0)
			}
		}
	}
	return agentParameters
}

func Server(ap *AgentParameters) {
	defer ap.wgServer.Done()
	var err error
	var conn *net.TCPConn
	if ap.listener, err = net.ListenTCP("tcp4", ap.GetTCPAddr()); err == nil {
		for loop := true; loop; {
			if conn, err = ap.listener.AcceptTCP(); err == nil {
				ap.connChan <- conn
				ap.logChan <- NewLogMessage(errors.New(fmt.Sprintf("New connection accepted from %s", conn.RemoteAddr().String())), LOG_CONNECT_ACCEPT)
			} else {
				switch err.(type) {
				case *net.OpError:
					if err.(*net.OpError).Err.Error() == "use of closed network connection" {
						loop = false
						ap.logChan <- NewLogMessage(errors.New("The listener has been closed."), LOG_NETWORK_ERR)
						break
					} else {
						ap.logChan <- NewLogMessage(err, LOG_NETWORK_ERR)
					}
				default:
					ap.logChan <- NewLogMessage(err, LOG_NETWORK_ERR)
				}
			}
		}
	} else {
		ap.logChan <- NewLogMessage(err, LOG_NETWORK_ERR)
		return
	}
}
func Worker(ap *AgentParameters, i int) {
	defer ap.wgWorker.Done()
	var log func(e interface{}, lv uint8) = func(e interface{}, lv uint8) {
		switch e.(type) {
		case error:
			ap.logChan <- NewLogMessage(e.(error), lv)
		case string:
			ap.logChan <- NewLogMessage(errors.New(e.(string)), lv)
		}
	}

	log(fmt.Sprintf("The worker %d starts.", i), LOG_PROCESS_EVENT)
	for loop := true; loop; {
		select {
		case conn := <-ap.connChan:
			var reader *bufio.Reader = bufio.NewReader(conn)
			var buff []byte = make([]byte, 0, 128)
			var err error
			if buff, err = reader.ReadBytes(10); err != nil {
				log(err, LOG_NETWORK_ERR)
				if err = conn.Close(); err != nil {
					log(err, LOG_NETWORK_ERR)
				}
				continue
			}
			// TODO: Handle incorrect HAProxy check request
			var pgParams *PgsqlConnectionParameters = ParseHAProxyRequest(ap, buff)
			log(fmt.Sprintf("Checking (h=%s,p=%d,db=%s,u=%s,rt=%s,wt=%s) for %s", pgParams.Host, pgParams.Port, pgParams.DbName, pgParams.Username, pgParams.Timeout, pgParams.Timeout, conn.RemoteAddr().String()), LOG_CHECK_REQ)

			log(fmt.Sprintf("Getting a DB connection for %s", conn.RemoteAddr().String()), LOG_CHECK_DEBUG)
			if GetDbConnection(pgParams); pgParams.dbConn == nil {
				if _, err = conn.Write([]byte("DOWN DOWN#NetworkError\n")); err != nil {
					log(err, LOG_NETWORK_ERR)
				}
				if err = pgParams.dbConn.Close(); err != nil {
					log(err, LOG_DB_ERR)
				}
				log(fmt.Sprintf("Send DOWN status for %s", conn.RemoteAddr().String()), LOG_CHECK_EVENT)
				if err = conn.Close(); err != nil {
					log(err, LOG_NETWORK_ERR)
				}
				continue
			} else {
				log(fmt.Sprintf("Got a DB connection as expected for %s", conn.RemoteAddr().String()), LOG_CHECK_DEBUG)
			}
			log(fmt.Sprintf("Checking a DB connection for %s", conn.RemoteAddr().String()), LOG_CHECK_DEBUG)
			if !CheckConnection(pgParams) {
				if _, err = conn.Write([]byte(fmt.Sprintf("DOWN DOWN#%s\n", pgParams.reason))); err != nil {
					log(err, LOG_NETWORK_ERR)
				}
				if err = pgParams.dbConn.Close(); err != nil {
					log(err, LOG_DB_ERR)
				}
				log(fmt.Sprintf("Send DONW status for %s", conn.RemoteAddr().String()), LOG_CHECK_EVENT)
				if err = conn.Close(); err != nil {
					log(err, LOG_NETWORK_ERR)
				}
				continue
			} else {
				log(fmt.Sprintf("The DB connection works expected for %s", conn.RemoteAddr().String()), LOG_CHECK_DEBUG)
			}
			log(fmt.Sprintf("Checking isMaster() for %s", conn.RemoteAddr().String()), LOG_CHECK_DEBUG)
			if !CheckIsMaster(pgParams) {
				if _, err = conn.Write([]byte("DOWN DOWN#NodeIsInRecovery\n")); err != nil {
					log(err, LOG_NETWORK_ERR)
				}
				if err = pgParams.dbConn.Close(); err != nil {
					log(err, LOG_DB_ERR)
				}
				log(fmt.Sprintf("Send DONW status for %s", conn.RemoteAddr().String()), LOG_CHECK_EVENT)
				if err = conn.Close(); err != nil {
					log(err, LOG_NETWORK_ERR)
				}
				continue
			} else {
				log(fmt.Sprintf("The host is Master for %s", conn.RemoteAddr().String()), LOG_CHECK_DEBUG)
			}
			log(fmt.Sprintf("Send UP status for %s", conn.RemoteAddr().String()), LOG_CHECK_EVENT)
			if _, err = conn.Write([]byte("UP UP#NodeIsMAster\n")); err != nil {
				log(err, LOG_NETWORK_ERR)
			}
			if err = pgParams.dbConn.Close(); err != nil {
				log(err, LOG_DB_ERR)
			}
			if err = conn.Close(); err != nil {
				log(err, LOG_NETWORK_ERR)
			}
		case <-ap.stopWorkerChan:
			loop = false
		}
	}
	log(fmt.Sprintf("The worker %d stops.", i), LOG_PROCESS_EVENT)
}
func GetDbConnection(pcp *PgsqlConnectionParameters) *sql.DB {
	//if db, err := sql.Open("postgres", pcp.String()); err != nil {
	if db, err := sql.Open("pq-timeouts", pcp.String()); err != nil {
		pcp.logChan <- NewLogMessage(err, LOG_DB_ERR)
		return nil
	} else {
		pcp.dbConn = db
		return db
	}
}
func CheckConnection(pcp *PgsqlConnectionParameters) bool {
	if err := pcp.dbConn.Ping(); err != nil {
		pcp.logChan <- NewLogMessage(err, LOG_DB_ERR)
		switch {
		case strings.HasPrefix(err.Error(), "pq: password authentication failed for user"):
			pcp.reason = "AuthFailed"
		case strings.HasPrefix(err.Error(), "connect: connection refused"):
			pcp.reason = "ConnectionRefused"
		case strings.HasSuffix(err.Error(), "does not exist"):
			pcp.reason = "DatabaseNameError"
		default:
			pcp.reason = "UnexpectedError"
		}
		return false
	} else {
		return true
	}
}
func CheckIsMaster(pcp *PgsqlConnectionParameters) bool {
	var row *sql.Row = pcp.dbConn.QueryRow("select pg_is_in_recovery()")
	var isSlave bool
	if err := row.Scan(&isSlave); err != nil {
		pcp.logChan <- NewLogMessage(err, LOG_DB_ERR)
		return false
	} else {
		return !isSlave
	}
}
func ParseHAProxyRequest(ap *AgentParameters, req []byte) *PgsqlConnectionParameters {
	var pcp *PgsqlConnectionParameters = new(PgsqlConnectionParameters)
	pcp.logChan = ap.logChan
	pcp.Timeout = ap.pgTimeout
	var reqStr string = strings.TrimSpace(string(req))
	for _, arg := range strings.Split(reqStr, DEFAULT_DELIMITER) {
		switch {
		case strings.HasPrefix(arg, "host="):
			pcp.Host = strings.SplitN(arg, "=", 2)[1]
		case strings.HasPrefix(arg, "port="):
			pcp.Port, _ = strconv.Atoi(strings.SplitN(arg, "=", 2)[1])
		case strings.HasPrefix(arg, "user="):
			pcp.Username = strings.SplitN(arg, "=", 2)[1]
		case strings.HasPrefix(arg, "pass="):
			pcp.Password = strings.SplitN(arg, "=", 2)[1]
		case strings.HasPrefix(arg, "dbname="):
			pcp.DbName = strings.SplitN(arg, "=", 2)[1]
		}
	}
	return pcp
}
func Logger(ap *AgentParameters) {
	defer ap.wgLogger.Done()
	var log func(ap *AgentParameters, lm *LogMessage)
	var runIn string
	if ap.runInDocker == "yes" {
		log = WriteLogToDocker
		runIn = "Agent runs in Docker."
	} else {
		log = WriteLogToFile
		runIn = "Agent doesn't run in Docker."
	}
	log(ap, NewLogMessage(errors.New("Agent starts."), LOG_PROCESS_EVENT))
	log(ap, NewLogMessage(errors.New(runIn), LOG_PROCESS_EVENT))
	var logLevel uint8
	if ll, err := strconv.Atoi(ap.logLevel); err != nil {
		log(ap, NewLogMessage(errors.New(fmt.Sprintf("Invalid log level specified (%s), fallback to the default (%s)", ap.logLevel, DEFAULT_LOG_LEVEL)), LOG_PROCESS_EVENT))
		ll, _ = strconv.Atoi(DEFAULT_LOG_LEVEL)
		logLevel = uint8(ll)
	} else {
		log(ap, NewLogMessage(errors.New(fmt.Sprintf("Log level set to %s", ap.logLevel)), LOG_PROCESS_EVENT))
		logLevel = uint8(ll)
	}

	for loop := true; loop; {
		select {
		case logMessage := <-ap.logChan:
			if logLevel&logMessage.LogLevel != 0 {
				log(ap, logMessage)
			}
		case <-ap.stopLoggerChan:
			loop = false
		}
	}
	log(ap, NewLogMessage(errors.New("Agent stops."), LOG_PROCESS_EVENT))
}
func WriteLogToDocker(ap *AgentParameters, lm *LogMessage) {
	fmt.Printf("%s: %s\n", time.Now(), lm.Error())
}
func WriteLogToFile(ap *AgentParameters, lm *LogMessage) {
	if fd, fdErr := os.OpenFile(ap.logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0775); fdErr == nil {
		if _, err := fd.WriteString(fmt.Sprintf("%s: %s\n", time.Now(), lm.Error())); err != nil {
			fmt.Println("LogFD write error !!")
			fmt.Println(err)
		}
		if err := fd.Sync(); err != nil {
			fmt.Println("LogFD sync error !!")
			fmt.Println(err)
		}
		if err := fd.Close(); err != nil {
			fmt.Println("LogFD close error !!")
			fmt.Println(err)
		}
	} else {
		fmt.Println("LogFD open error !!")
		fmt.Println(fdErr.Error())
	}
}
func StopAllRoutines(ap *AgentParameters) {
	defer ap.wgKiller.Done()
	for i := 0; i < ap.GetNbWorker(); i++ {
		ap.stopWorkerChan <- true
	}
	ap.wgWorker.Wait()
	ap.wgServer.Wait()
	ap.stopLoggerChan <- true
	ap.wgLogger.Wait()
}
