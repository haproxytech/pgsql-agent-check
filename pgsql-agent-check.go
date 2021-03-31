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
	_ "github.com/lib/pq"
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
const HELP string = `Help :
--address  The agent listens on this address (default %s)
--port     The agent listens on this port (default %s)
--worker   The agent runs N worker (default %s)
--logfile  The agent writes its log to this file (default %s)
`

type PgsqlConnectionParameters struct {
	Host     string
	Port     int
	Username string
	Password string
	DbName   string
	logChan  chan error
	dbConn   *sql.DB
	reason   string
}

func (pcp *PgsqlConnectionParameters) String() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", pcp.Host, pcp.Port, pcp.Username, pcp.Password, pcp.DbName)
}

type AgentParameters struct {
	address        string
	port           string
	logFile        string
	worker         string
	logChan        chan error
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
		ap.logChan <- err
		ap.logChan <- errors.New("Fallback to the default address (" + DEFAULT_ADDRESS + ").")
		address.UnmarshalText([]byte(DEFAULT_ADDRESS))
	}
	if port, err = strconv.Atoi(ap.port); err != nil {
		ap.logChan <- err
		ap.logChan <- errors.New("Fallback to the default address (" + DEFAULT_PORT + ").")
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
		ap.logChan <- err
		ap.logChan <- errors.New("Fallback to the default number of worker(" + DEFAULT_WORKER + ").")
		nbWorker, _ = strconv.Atoi(DEFAULT_WORKER)
	}
	return nbWorker
}
func NewAgentParameters() *AgentParameters {
	var ap *AgentParameters = new(AgentParameters)
	ap.address = DEFAULT_ADDRESS
	ap.port = DEFAULT_PORT
	ap.logFile = DEFAULT_LOG_FILE
	ap.worker = DEFAULT_WORKER
	ap.logChan = make(chan error)
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
	go LogError(agentParameters)
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
		agentParameters.logChan <- errors.New("HAProxy reloads.")
		agentParameters.listener.Close()
		agentParameters.wgKiller.Add(1)
		StopAllRoutines(agentParameters)
	case syscall.SIGTERM:
		agentParameters.logChan <- errors.New("HAProxy stops/restarts.")
		agentParameters.listener.Close()
		agentParameters.wgKiller.Add(1)
		StopAllRoutines(agentParameters)
	default:
		agentParameters.logChan <- errors.New(fmt.Sprintf("Received %s", fmt.Sprint(sig)))
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
			case "--help":
				fmt.Printf(HELP, DEFAULT_ADDRESS, DEFAULT_PORT, DEFAULT_WORKER, DEFAULT_LOG_FILE)
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
			} else {
				switch err.(type) {
				case *net.OpError:
					if err.(*net.OpError).Err.Error() == "use of closed network connection" {
						loop = false
						ap.logChan <- errors.New("The listener has been closed.")
						break
					} else {
						ap.logChan <- err
					}
				default:
					ap.logChan <- err
				}
			}
		}
	} else {
		ap.logChan <- err
		return
	}
}
func Worker(ap *AgentParameters, i int) {
	defer ap.wgWorker.Done()
	ap.logChan <- errors.New(fmt.Sprintf("The worker %d starts.", i))
	for loop := true; loop; {
		select {
		case conn := <-ap.connChan:
			var reader *bufio.Reader = bufio.NewReader(conn)
			var buff []byte = make([]byte, 0, 128)
			var err error
			buff, err = reader.ReadBytes(10)
			var pgParams *PgsqlConnectionParameters = ParseHAProxyRequest(ap, buff)

			if GetDbConnection(pgParams); pgParams.dbConn == nil {
				if _, err = conn.Write([]byte("DOWN DOWN#NetworkError\n")); err != nil {
					ap.logChan <- err
				}
				pgParams.dbConn.Close()
				break
			}
			if !CheckConnection(pgParams) {
				if _, err = conn.Write([]byte(fmt.Sprintf("DOWN DOWN#%s\n", pgParams.reason))); err != nil {
					ap.logChan <- err
				}
				pgParams.dbConn.Close()
				break
			}
			if !CheckIsMaster(pgParams) {
				if _, err = conn.Write([]byte("DOWN DOWN#NodeIsInRecovery\n")); err != nil {
					ap.logChan <- err
				}
				pgParams.dbConn.Close()
				break
			}
			if _, err = conn.Write([]byte("UP UP#NodeIsMAster\n")); err != nil {
				ap.logChan <- err
			}
			pgParams.dbConn.Close()
			if err = conn.Close(); err != nil {
				ap.logChan <- err
			}
		case <-ap.stopWorkerChan:
			loop = false
		}
	}
	ap.logChan <- errors.New(fmt.Sprintf("The worker %d stops.", i))
}
func GetDbConnection(pcp *PgsqlConnectionParameters) *sql.DB {
	if db, err := sql.Open("postgres", pcp.String()); err != nil {
		pcp.logChan <- err
		return nil
	} else {
		pcp.dbConn = db
		return db
	}
}
func CheckConnection(pcp *PgsqlConnectionParameters) bool {
	if err := pcp.dbConn.Ping(); err != nil {
		pcp.logChan <- err
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
		pcp.logChan <- err
		return false
	} else {
		return !isSlave
	}
}
func ParseHAProxyRequest(ap *AgentParameters, req []byte) *PgsqlConnectionParameters {
	var pcp *PgsqlConnectionParameters = new(PgsqlConnectionParameters)
	pcp.logChan = ap.logChan
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
func LogError(ap *AgentParameters) {
	defer ap.wgLogger.Done()
	if fd, fdErr := os.OpenFile(ap.logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0775); fdErr == nil {
		fd.WriteString(fmt.Sprintf("%s: Agent starts.\n", time.Now()))
		fd.Close()
	}
	for loop := true; loop; {
		select {
		case err := <-ap.logChan:
			if fd, fdErr := os.OpenFile(ap.logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0775); fdErr == nil {
				fd.WriteString(fmt.Sprintf("%s: %s\n", time.Now(), err.Error()))
				fd.Close()
			}
		case <-ap.stopLoggerChan:
			loop = false
		}
	}
	if fd, fdErr := os.OpenFile(ap.logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0775); fdErr == nil {
		fd.WriteString(fmt.Sprintf("%s: Agent stops.\n", time.Now()))
		fd.Close()
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