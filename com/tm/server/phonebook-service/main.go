// phonebook-service: nhận danh bạ từ App theo bucket, lưu HBase, gửi thay đổi lên Kafka (PHONEBOOK.md).
//
//	App ──HTTP──► router ─► handler ─► controller ─► dao ─► HBase `phonebook`
//	                                                   └──► Kafka phonebook_service_events ──► event-gateway ──► StarRocks
//
// MainServer (main.go, app.go, http_server.go): đọc config, dựng DI (go.uber.org/fx) từ module các layer,
// start theo thứ tự hbase → kafka → http, dừng ngược lại khi SIGINT / SIGTERM.
package main

import (
	"fmt"
	"os"

	"go.uber.org/fx"

	"thor/server/phonebook-service/internal/configs"
)

func main() {
	cfg, err := configs.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	fx.New(Options(cfg)).Run()
}
