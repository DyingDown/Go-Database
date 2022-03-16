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

	f, err := os.OpenFile("test.sql", os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	input := bufio.NewReader(f)
	// input := bufio.NewReader(os.Stdin)
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	xid := vm.NULL_Xid
	for {
		fmt.Print("go-db> ")
		sql := ""
		for {
			line, err := input.ReadString('\n')
			// fmt.Print(line)
			if err != nil {
				if err == io.EOF {
					break
				}
				logrus.Error(err)
				return
			}
			if line == "exit;\n" {
				return
			}
			sql += line
			if line[len(line)-2] == ';' {
				break
			}
			fmt.Print("    >>> ")

		}

		if err != nil && err != io.EOF {
			logrus.Error(err)
			return
		}
		// fmt.Print(sql)
		request := &transporter.Request{
			Xid: xid,
			SQL: sql,
		}

		err := enc.Encode(request)
		if err != nil {
			logrus.Error(err)
			return
		}
		logrus.Info("success encode")
		response := &transporter.Response{}
		err = dec.Decode(response)
		if err != nil {
			logrus.Error(err)
			return
		}
		xid = response.Xid
		if response.Err != "" {
			logrus.Error(response.Err)
		}
		if response.ResultList != nil {
			fmt.Print(response.ResultList.String())
		}

	}

}
