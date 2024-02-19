package hadoop_streaming

import (
	"flag"
	"fmt"
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

func (app *Application) Run() {
	reducerFlag := flag.Bool("reducer", false, "")
	flag.Parse()
	var err error
	if *reducerFlag {
		if app.reducer == nil {
			err = fmt.Errorf("reducer is nil")
		} else {
			err = app.reducer()
		}
	} else {
		if app.mapper == nil {
			err = fmt.Errorf("mappper is nil")
		} else {
			err = app.mapper()
		}
	}
	if err != nil {
		println(err.Error())
	}
}
