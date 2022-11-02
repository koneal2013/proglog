package main

import (
	"log"

	"github.com/koneal2013/proglog/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatalln(srv.ListenAndServe())
}
