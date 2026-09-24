package router

import "go.uber.org/fx"

// Module đăng ký: http.Handler của API.
var Module = fx.Module("router", fx.Provide(New))
