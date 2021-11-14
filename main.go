package main

import (
	"Go-Database/parser"
	"fmt"
)

func main() {
	sqlParser := parser.NewTokenizer("Create table color(red int, green string, blue float);")
	fmt.Println(sqlParser.Tokens)
}
