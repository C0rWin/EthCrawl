package log

import (
	"os"

	"github.com/sirupsen/logrus"
)

// Logger is a wrapper around logrus.Logger
type Logger struct {
	name string
	*logrus.Logger
}

// NewLogger creates a new logger with the given name
func NewLogger(name string, level logrus.Level) *Logger {
	// create a new logger
	logger := logrus.New()

	// set the logger level and output formatter
	logger.SetFormatter(&logrus.TextFormatter{})
	logger.SetLevel(level)
	logger.SetOutput(os.Stdout)

	return &Logger{
		name:   name,
		Logger: logger,
	}
}
