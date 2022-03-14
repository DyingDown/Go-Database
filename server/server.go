package server

import (
	"encoding/gob"
	"fmt"
	"go-database/parser"
	"go-database/tbm"
	"go-database/transporter"
	"go-database/util"
	"net"

	"go-database/parser/ast"

	"github.com/sirupsen/logrus"
)

type Server struct {
	TableManager *tbm.TableManager
}

func NewServer(isOpen bool, isCreate bool, path string) (*Server, error) {
	server := &Server{}
	if isOpen && isCreate {
		return nil, fmt.Errorf("open and create can't exist at the same time")
	}
	if isOpen {
		server.TableManager = tbm.Open(path)
	}
	if isCreate {
		server.TableManager = tbm.Create(path)
	}
	return server, nil
}

func (server *Server) Start() {
	go func() {
		fmt.Println("start as server")
		listener, err := net.Listen(util.NetWork, util.Address)
		if err != nil {
			logrus.Errorf("listen error: %s", err)
			return
		}
		defer func() {
			listener.Close()
			server.TableManager.Close()
		}()
		fmt.Println("server start")
		for {
			conn, err := listener.Accept()
			if err != nil {
				logrus.Errorf("accept error: %s", err)
				return
			}
			go server.handle(conn)
		}
	}()
	WaitToExit()
}

func (server *Server) handle(conn net.Conn) {
	defer conn.Close()
	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)
	for {
		request := &transporter.Request{}
		err := decoder.Decode(&request)
		if err != nil {
			logrus.Errorf("decode request error: %s", err)
			return
		}
		response := server.HandleRequest(request)
		err = encoder.Encode(response)
		if err != nil {
			logrus.Errorf("encode response error: %s", err)
			return
		}
	}
}

func (server *Server) HandleRequest(request *transporter.Request) *transporter.Response {
	response := &transporter.Response{}
	response.Xid = request.Xid
	stmt := parser.ParseStatement(request.SQL)
	var xid uint64 = request.Xid
	var err error
	switch stmt := stmt.(type) {
	case *ast.SQLCreateTableStatement:
		err = server.TableManager.CreateTable(request.Xid, stmt)
	case *ast.SQLInsertStatement:
		response.ResultList, err = server.TemporaryTransaction(xid, func(xid uint64) (*tbm.Result, error) {
			return server.TableManager.Insert(xid, stmt)
		})
	case *ast.SQLSelectStatement:
		response.ResultList, err = server.TemporaryTransaction(xid, func(xid uint64) (*tbm.Result, error) {
			return server.TableManager.Select(xid, stmt)
		})
	case *ast.SQLUpdateStatement:
		response.ResultList, err = server.TemporaryTransaction(xid, func(xid uint64) (*tbm.Result, error) {
			return server.TableManager.Update(xid, stmt)
		})
	case *ast.SQLDeleteStatement:
		response.ResultList, err = server.TemporaryTransaction(xid, func(xid uint64) (*tbm.Result, error) {
			return server.TableManager.Delete(xid, stmt)
		})
	case *ast.BeginTransaction:
		response.Xid = server.TableManager.Begin()
	case *ast.AbortTransaction:
		server.TableManager.Abort(request.Xid)
		response.Xid = 0
	case *ast.CommitTransaction:
		server.TableManager.Commit(request.Xid)
		response.Xid = 0
	default:
		response.Err = fmt.Sprintf("unsupported statement: %s", request.SQL)
	}
	if err != nil {
		response.Err = err.Error()
	}
	return response
}

func (server *Server) TemporaryTransaction(xid uint64, f func(xid uint64) (*tbm.Result, error)) (*tbm.Result, error) {
	var isTT bool = false
	if xid == 0 {
		isTT = true
		xid = server.TableManager.Begin()
	}
	result, err := f(xid)
	if isTT {
		server.TableManager.Commit(xid)
	}
	return result, err
}

func WaitToExit() {
	exit := make(chan bool)

	go func() {
		var input string
		for {
			_, err := fmt.Scanln(&input)
			if err != nil {
				logrus.Errorf("scan error: %s", err)
				return
			}
			if input == "exit" {
				exit <- true
				return
			}
		}
	}()

	<-exit
	fmt.Println("exit")
}
