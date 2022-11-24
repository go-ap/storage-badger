package badger

type logger struct {
	logFn loggerFn
	errFn loggerFn
}

func (l logger) Errorf(s string, p ...interface{}) {
	l.errFn(s, p...)
}
func (l logger) Warningf(s string, p ...interface{}) {
	l.errFn(s, p...)
}
func (l logger) Infof(s string, p ...interface{}) {
	l.logFn(s, p...)
}
func (l logger) Debugf(s string, p ...interface{}) {
	l.logFn(s, p...)
}
