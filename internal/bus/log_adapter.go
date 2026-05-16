package bus

import "fmt"

// natsLogAdapter implements natsserver/server.Logger by forwarding to our Logger.
type natsLogAdapter struct{ l Logger }

func (a *natsLogAdapter) Noticef(format string, v ...any) { a.l.Info(fmt.Sprintf(format, v...)) }
func (a *natsLogAdapter) Warnf(format string, v ...any)   { a.l.Warn(fmt.Sprintf(format, v...)) }
func (a *natsLogAdapter) Fatalf(format string, v ...any)  { a.l.Error(fmt.Sprintf(format, v...)) }
func (a *natsLogAdapter) Errorf(format string, v ...any)  { a.l.Error(fmt.Sprintf(format, v...)) }
func (a *natsLogAdapter) Debugf(format string, v ...any)  { a.l.Debug(fmt.Sprintf(format, v...)) }
func (a *natsLogAdapter) Tracef(format string, v ...any)  { a.l.Debug(fmt.Sprintf(format, v...)) }
