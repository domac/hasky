package app

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
)

type logWriter struct {
	Logger
}

func (l logWriter) Write(p []byte) (int, error) {
	l.Logger.Output(2, string(p))
	return len(p), nil

}

//http服务
func Serve(listener net.Listener, handler http.Handler, proto string, l Logger) {
	l.Output(2, fmt.Sprintf("%s: listening on %s", proto, listener.Addr()))

	server := &http.Server{
		Handler:  handler,
		ErrorLog: log.New(logWriter{}, "", 0)}

	err := server.Serve(listener)
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		l.Output(2, fmt.Sprintf("ERROR: http.Serve() - %s", err))
	}
	l.Output(2, fmt.Sprintf("%s: closing %s", proto, listener.Addr()))
}
