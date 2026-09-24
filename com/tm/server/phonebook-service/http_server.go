package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"go.uber.org/fx"
)

type httpSettings struct {
	Port int
}

type httpServer struct {
	srv *http.Server
}

// newHTTPServer: lifecycle listen khi start (sau hbase, kafka), Shutdown khi stop (trước kafka, hbase).
func newHTTPServer(lc fx.Lifecycle, s httpSettings, h http.Handler, log *slog.Logger, shutdowner fx.Shutdowner) *httpServer {
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", s.Port),
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ln, err := net.Listen("tcp", srv.Addr)
			if err != nil {
				return err
			}
			log.Info("listening", "port", s.Port)
			go func() {
				if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Error("http server failed", "err", err)
					_ = shutdowner.Shutdown(fx.ExitCode(1))
				}
			}()
			return nil
		},
		OnStop: srv.Shutdown,
	})
	return &httpServer{srv: srv}
}
