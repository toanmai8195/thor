package handler

import "go.uber.org/fx"

// Module đăng ký: *PhonebookHandler, *HealthHandler (Pinger do MainServer cung cấp).
var Module = fx.Module("handler",
	fx.Provide(NewPhonebookHandler, NewHealthHandler),
)
