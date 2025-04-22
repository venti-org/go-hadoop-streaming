package hadoop_streaming

import (
	"fmt"
)

const (
	MODE_MAPPER  = "mapper"
	MODE_REDUCER = "reducer"
)

type Runner = func() error

type Application struct {
	mapper  Runner
	reducer Runner
}

func NewApplication() *Application {
	return &Application{}
}

func (app *Application) WithMapper(mapper Runner) *Application {
	app.mapper = mapper
	return app
}

func (app *Application) WithReducer(reducer Runner) *Application {
	app.reducer = reducer
	return app
}

func (app *Application) Run(mode string) error {
	switch mode {
	case MODE_MAPPER:
		if app.mapper == nil {
			return fmt.Errorf("mappper is nil")
		} else {
			return app.mapper()
		}
	case MODE_REDUCER:
		if app.reducer == nil {
			return fmt.Errorf("reducer is nil")
		} else {
			return app.reducer()
		}
	}
	return fmt.Errorf("unknown mode: %s", mode)
}
