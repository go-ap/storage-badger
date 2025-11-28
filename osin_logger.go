package badger

type logger struct {
	logFn loggerFn
	errFn loggerFn
}

func (l logger) Errorf(s string, p ...interface{}) {
	if l.errFn == nil {
		return
	}
	l.errFn(s, p...)
}
func (l logger) Warningf(s string, p ...interface{}) {
	if l.errFn == nil {
		return
	}
	l.errFn(s, p...)
}
func (l logger) Infof(s string, p ...interface{}) {
	if l.logFn == nil {
		return
	}
	l.logFn(s, p...)
}
func (l logger) Debugf(s string, p ...interface{}) {
	if l.logFn == nil {
		return
	}
	l.logFn(s, p...)
}
