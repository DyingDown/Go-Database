package main

import (
	"flag"
	"fmt"
	"go-database/client"
	"go-database/server"
	"go-database/util"
	"os"

	log "github.com/sirupsen/logrus"
)

// 设置 log 输出格式
func init() {
	//设置output,默认为stderr,可以为任何io.Writer，比如文件*os.File
	log.SetOutput(os.Stdout)
	//设置最低loglevel
	log.SetLevel(log.InfoLevel)
	log.SetReportCaller(true)
	log.SetFormatter(&util.MyFormatter{})
}

func main() {
	// sql := "Create table color(red int, green string, blue float);"

	isServer := flag.Bool("server", false, "run as server")
	isClient := flag.Bool("client", false, "run as client")
	isCreate := flag.Bool("create", false, "create database")
	isOpen := flag.Bool("open", false, "open database")
	path := flag.String("path", "", "database path")
	flag.Parse()

	if *isOpen && *isCreate {
		fmt.Println("open and create can't exist at the same time")
		return
	}
	if *isServer {
		server, err := server.NewServer(*isOpen, *isCreate, *path)
		if err != nil {
			panic(err)
		}
		server.Start()
	} else if *isClient {
		client := client.NewClient()
		client.Start()
	} else {
		panic("Not specify server or client")
	}
	// parser.ParseStatement(sql)
}
