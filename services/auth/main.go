package auth

import (
	"runtime/debug"

	"github.com/gofiber/fiber/v3"
	fiberlogger "github.com/gofiber/fiber/v3/middleware/logger"
	"github.com/gofiber/fiber/v3/middleware/recover"
	"github.com/mirstar13/go-map-reduce/pkg/logger"
	"go.uber.org/zap"
)

func main() {
	log, err := logger.New()
	if err != nil {
		panic(err)
	}
	defer log.Sync()

	app := fiber.New()

	// Pass log into the recover middleware
	app.Use(recover.New(recover.Config{
		EnableStackTrace: true,
		StackTraceHandler: func(c fiber.Ctx, e any) {
			log.Error("panic recovered",
				zap.Any("error", e),
				zap.String("method", c.Method()),
				zap.String("path", c.Path()),
				zap.ByteString("stack", debug.Stack()),
			)
		},
	}))

	app.Use(fiberlogger.New(fiberlogger.Config{
		LoggerFunc: func(c fiber.Ctx, data *fiberlogger.Data, cfg *fiberlogger.Config) error {
			log.Info("request",
				zap.String("method", c.Method()),
				zap.String("path", c.Path()),
				zap.String("ip", c.IP()),
				zap.Duration("latency", data.Stop.Sub(data.Start)),
			)
			return nil
		},
	}))

	app.Listen(":8082")
}
