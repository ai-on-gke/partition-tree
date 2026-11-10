/*
Copyright 2026 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import "testing"

func TestParseMIGSelfLink(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    MIGRef
		wantErr bool
	}{
		{
			name: "basic",
			in:   "projects/123/zones/us-central1-a/instanceGroupManagers/foo",
			want: MIGRef{ProjectNumber: "123", Zone: "us-central1-a", Name: "foo"},
		},
		{
			name: "leading-slash",
			in:   "/projects/123/zones/us-central1-a/instanceGroupManagers/foo",
			want: MIGRef{ProjectNumber: "123", Zone: "us-central1-a", Name: "foo"},
		},
		{
			name: "with-compute-v1-prefix",
			in:   "https://www.googleapis.com/compute/v1/projects/123/zones/us-central1-a/instanceGroupManagers/foo",
			want: MIGRef{ProjectNumber: "123", Zone: "us-central1-a", Name: "foo"},
		},
		{
			name: "with-compute-beta-prefix",
			in:   "https://compute.googleapis.com/compute/beta/projects/42/zones/europe-west4-a/instanceGroupManagers/bar-mig",
			want: MIGRef{ProjectNumber: "42", Zone: "europe-west4-a", Name: "bar-mig"},
		},
		{
			name:    "region-instead-of-zone",
			in:      "projects/123/regions/us-central1/instanceGroupManagers/foo",
			wantErr: true,
		},
		{
			name:    "too-few-segments",
			in:      "projects/123",
			wantErr: true,
		},
		{
			name:    "empty-project-segment",
			in:      "projects//zones/us-central1-a/instanceGroupManagers/foo",
			wantErr: true,
		},
		{
			name:    "empty-zone-segment",
			in:      "projects/123/zones//instanceGroupManagers/foo",
			wantErr: true,
		},
		{
			name:    "empty-mig-segment",
			in:      "projects/123/zones/us-central1-a/instanceGroupManagers/",
			wantErr: true,
		},
		{
			name:    "empty-string",
			in:      "",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseMIGSelfLink(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			if err == nil && got != tc.want {
				t.Errorf("got %+v want %+v", got, tc.want)
			}
		})
	}
}
