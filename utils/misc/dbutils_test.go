package misc_test

import (
	"github.com/rudderlabs/rudder-server/utils/misc"
	"testing"
)

func TestSetApplicationNameInDBConnection(t *testing.T) {
	type args struct {
		dns     string
		appName string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "invalid dns url",
			args: args{
				dns:     "abc@example.com:5432",
				appName: "rsources",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "add app name in dns url",
			args: args{
				dns:     "postgresql://rudder:password@prousmtusmt-rs-shared-postgresql:5432/jobsdb?sslmode=disable",
				appName: "rsources",
			},
			want:    "postgresql://rudder:password@prousmtusmt-rs-shared-postgresql:5432/jobsdb?application_name=rsources&sslmode=disable",
			wantErr: false,
		},
		{
			name: "update app name in dns url",
			args: args{
				dns:     "postgresql://rudder:password@prousmtusmt-rs-shared-postgresql:5432/jobsdb?application_name=random&sslmode=disable",
				appName: "rsources",
			},
			want:    "postgresql://rudder:password@prousmtusmt-rs-shared-postgresql:5432/jobsdb?application_name=rsources&sslmode=disable",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := misc.SetApplicationNameInDBConnectionURL(tt.args.dns, tt.args.appName)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetApplicationNameInDBConnectionURL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SetApplicationNameInDBConnectionURL() got = %v, want %v", got, tt.want)
			}
		})
	}
}
