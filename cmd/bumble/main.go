package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	concurrency "github.com/brown-csci1270/db/pkg/concurrency"
	config "github.com/brown-csci1270/db/pkg/config"
	db "github.com/brown-csci1270/db/pkg/db"
	list "github.com/brown-csci1270/db/pkg/list"
	pager "github.com/brown-csci1270/db/pkg/pager"
	query "github.com/brown-csci1270/db/pkg/query"
	recovery "github.com/brown-csci1270/db/pkg/recovery"
	repl "github.com/brown-csci1270/db/pkg/repl"

	uuid "github.com/google/uuid"
)

// Default port 8335 (BEES).
const DEFAULT_PORT int = 8335

// Listens for SIGINT or SIGTERM and calls table.CloseDB().
func setupCloseHandler(database *db.Database) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("closehandler invoked")
		database.Close()
		os.Exit(0)
	}()
}

// Start listening for connections at port `port`.
func startServer(repl *repl.REPL, tm *concurrency.TransactionManager, prompt string, port int) {
	// Handle a connection by running the repl on it.
	handleConn := func(c net.Conn) {
		clientId := uuid.New()
		defer c.Close()
		if tm != nil {
			defer tm.Commit(clientId)
		}
		repl.Run(c, clientId, prompt)
	}
	// Start listening for new connections.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		log.Fatal(err)
	}
	dbName := config.DBName
	fmt.Printf("%v server started listening on localhost:%v\n", dbName,
		listener.Addr().(*net.TCPAddr).Port)
	// Handle each connection.
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Print(err)
			continue
		}
		go handleConn(conn)
	}
}

// Start the database.
func main() {
	// Set up flags.
	var dbFlag = flag.String("db", "data/", "DB folder")
	var portFlag = flag.Int("p", DEFAULT_PORT, "port number")
	var promptFlag = flag.Bool("c", true, "use prompt?")
	var projectFlag = flag.String("project", "", "choose project: [go,pager,db,query,concurrency,recovery] (required)")
	flag.Parse()
	// Open the db; if recovery, prime the database.
	var database *db.Database
	var err error
	if *projectFlag == "recovery" {
		database, err = recovery.Prime(*dbFlag)
	} else {
		database, err = db.Open(*dbFlag)
	}
	if err != nil {
		panic(err)
	}
	// Set up the log file.
	err = database.CreateLogFile(config.LogFileName)
	if err != nil {
		panic(err)
	}
	// Setup close conditions.
	defer database.Close()
	setupCloseHandler(database)
	// Set up REPL resources.
	prompt := config.GetPrompt(*promptFlag)
	repls := make([]*repl.REPL, 0)
	var tm *concurrency.TransactionManager
	var rm *recovery.RecoveryManager
	server := false
	// Get the right REPLs.
	switch *projectFlag {
	case "go":
		l := list.NewList()
		repls = append(repls, list.ListRepl(l))
	case "pager":
		pRepl, err := pager.PagerRepl()
		if err != nil {
			fmt.Println(err)
			return
		}
		repls = append(repls, pRepl)
	case "db":
		repls = append(repls, db.DatabaseRepl(database))
	case "query":
		repls = append(repls, db.DatabaseRepl(database))
		repls = append(repls, query.QueryRepl(database))
	case "concurrency":
		server = true
		lm := concurrency.NewLockManager()
		tm = concurrency.NewTransactionManager(lm)
		repls = append(repls, concurrency.TransactionREPL(database, tm))
	case "recovery":
		server = true
		lm := concurrency.NewLockManager()
		tm = concurrency.NewTransactionManager(lm)
		rm, err = recovery.NewRecoveryManager(database, tm, config.LogFileName)
		if err != nil {
			fmt.Println(err)
			return
		}
		repls = append(repls, recovery.RecoveryREPL(database, tm, rm))
		// Recover in this case!
		err = rm.Recover()
		if err != nil {
			fmt.Println(err)
			fmt.Println("Potentially corrupted write-ahead log --- unable to recover")
			fmt.Println("Consider clearing/fixing the log, or dropping down to a lower-level repl, e.g. the Concurrency repl")
			return
		}
	default:
		fmt.Println("must specify -project [go,pager,db,query,concurrency,recovery]")
		return
	}
	// Combine the REPLs.
	r, err := repl.CombineRepls(repls)
	if err != nil {
		fmt.Println(err)
		return
	}
	// Start server if server (concurrency or recovery), else run REPL here.
	if server {
		startServer(r, tm, prompt, *portFlag)
	} else {
		r.Run(nil, uuid.New(), prompt)
	}
}
