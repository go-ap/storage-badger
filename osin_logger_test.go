package badger

import "testing"

func Test_logger_Debugf(t *testing.T) {
	type fields struct {
		logFn loggerFn
		errFn loggerFn
	}
	type args struct {
		s string
		p []interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name:   "empty",
			fields: fields{},
			args:   args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := logger{
				logFn: tt.fields.logFn,
				errFn: tt.fields.errFn,
			}
			l.Debugf(tt.args.s, tt.args.p...)
		})
	}
}

func Test_logger_Errorf(t *testing.T) {
	type fields struct {
		logFn loggerFn
		errFn loggerFn
	}
	type args struct {
		s string
		p []interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name:   "empty",
			fields: fields{},
			args:   args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := logger{
				logFn: tt.fields.logFn,
				errFn: tt.fields.errFn,
			}
			l.Errorf(tt.args.s, tt.args.p...)
		})
	}
}

func Test_logger_Infof(t *testing.T) {
	type fields struct {
		logFn loggerFn
		errFn loggerFn
	}
	type args struct {
		s string
		p []interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name:   "empty",
			fields: fields{},
			args:   args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := logger{
				logFn: tt.fields.logFn,
				errFn: tt.fields.errFn,
			}
			l.Infof(tt.args.s, tt.args.p...)
		})
	}
}

func Test_logger_Warningf(t *testing.T) {
	type fields struct {
		logFn loggerFn
		errFn loggerFn
	}
	type args struct {
		s string
		p []interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name:   "empty",
			fields: fields{},
			args:   args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := logger{
				logFn: tt.fields.logFn,
				errFn: tt.fields.errFn,
			}
			l.Warningf(tt.args.s, tt.args.p...)
		})
	}
}
