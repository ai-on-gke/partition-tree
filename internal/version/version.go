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

// Package version provides build and version metadata for the PartitionTree controller.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

var (
	// GitVersion can be overridden at build time via -ldflags.
	GitVersion = "v0.0.0-unset"
	// GitCommit can be overridden at build time via -ldflags.
	GitCommit = ""
	// BuildDate can be overridden at build time via -ldflags.
	BuildDate = ""
)

// Info holds version and build metadata matching standard Kubernetes and GKE build_info schema.
type Info struct {
	GitVersion string
	GitCommit  string
	BuildDate  string
	GoVersion  string
	Compiler   string
	Platform   string
}

// Get returns the build and version information, populating unset fields from runtime/debug.
func Get() Info {
	ver := GitVersion
	commit := GitCommit
	date := BuildDate

	if bi, ok := debug.ReadBuildInfo(); ok {
		if ver == "v0.0.0-unset" && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
			ver = bi.Main.Version
		}
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				if commit == "" {
					commit = s.Value
				}
			case "vcs.time":
				if date == "" {
					date = s.Value
				}
			}
		}
	}

	return Info{
		GitVersion: ver,
		GitCommit:  commit,
		BuildDate:  date,
		GoVersion:  runtime.Version(),
		Compiler:   runtime.Compiler,
		Platform:   fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}
