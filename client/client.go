package client

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"go-database/transporter"
	"go-database/util"
	"go-database/vm"
	"io"
	"net"
	"os"

	"github.com/sirupsen/logrus"
)

type Client struct{}

func NewClient() *Client {
	return &Client{}
}

func (c *Client) Start() {
	fmt.Println("start as client")
	conn, err := net.Dial(util.NetWork, util.Address)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	input := bufio.NewReader(os.Stdin)
	encodder := gob.NewEncoder(conn)
	decodder := gob.NewDecoder(conn)

	xid := vm.NULL_Xid
	for fmt.Print("go-db> "); ; {
		sql := ""
		for line, err := input.ReadString('\n'); ((line != "" && line[len(line)-1] != '\n') || line == "") && err != nil; {
			fmt.Print("    >>> ")
			sql += line
		}
		if err != io.EOF {
			logrus.Error(err)
			return
		}
		if sql == "exit;" {
			return
		}
		request := &transporter.Request{
			Xid: xid,
			SQL: sql,
		}
		err := encodder.Encode(request)
		if err != nil {
			logrus.Error(err)
			return
		}
		response := &transporter.Response{}
		err = decodder.Decode(response)
		if err != nil {
			logrus.Error(err)
			return
		}
		xid = response.Xid
		if response.Err != "" {
			logrus.Error(response.Err)
			return
		}
		if response.ResultList != nil {
			response.ResultList.Print()
		}

	}

}
