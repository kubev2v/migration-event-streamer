package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func SetupLogger(format, level string) *zap.Logger {
	lvl, err := zapcore.ParseLevel(level)
	if err != nil {
		lvl = zapcore.InfoLevel
	}

	encoding := "console"
	if format == "json" {
		encoding = "json"
	}

	loggerCfg := &zap.Config{
		Level:    zap.NewAtomicLevelAt(lvl),
		Encoding: encoding,
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "time",
			LevelKey:       "severity",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "message",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeTime:     zapcore.RFC3339TimeEncoder,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeDuration: zapcore.MillisDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	plain, err := loggerCfg.Build(zap.AddStacktrace(zap.DPanicLevel))
	if err != nil {
		panic(err)
	}

	return plain
}
