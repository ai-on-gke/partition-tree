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

import (
	"fmt"
	"strings"
)

// MIGRef is the parsed form of a GCE MIG self-link, carrying the three
// fields needed for a compute API call.
type MIGRef struct {
	ProjectNumber string
	Zone          string
	Name          string
}

// parseMIGSelfLink parses a MIG self-link in the form
//
//	projects/<project_number>/zones/<zone>/instanceGroupManagers/<mig>
//
// A leading https://www.googleapis.com/compute/v1/ (or /compute/beta/) prefix
// is tolerated and stripped. Any other shape returns an error.
func parseMIGSelfLink(s string) (MIGRef, error) {
	raw := s
	if i := strings.Index(s, "/projects/"); i >= 0 {
		s = s[i+1:]
	}
	s = strings.TrimPrefix(s, "/")
	parts := strings.Split(s, "/")
	if len(parts) != 6 ||
		parts[0] != "projects" ||
		parts[2] != "zones" ||
		parts[4] != "instanceGroupManagers" {
		return MIGRef{}, fmt.Errorf("invalid MIG self-link %q", raw)
	}
	if parts[1] == "" || parts[3] == "" || parts[5] == "" {
		return MIGRef{}, fmt.Errorf("invalid MIG self-link %q: empty segment", raw)
	}
	return MIGRef{
		ProjectNumber: parts[1],
		Zone:          parts[3],
		Name:          parts[5],
	}, nil
}
