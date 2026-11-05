// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// TestRootPackageReExportsEveryExportedName keeps the root aquifer.go (the
// public package) in step with this package: every exported top-level name
// here must be re-exported there, or library users silently lose it.
func TestRootPackageReExportsEveryExportedName(t *testing.T) {
	core := exportedNames(t, ".", func(name string) bool {
		return strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go")
	})
	root := exportedNames(t, "../..", func(name string) bool { return name == "aquifer.go" })
	var missing, extra []string
	for n := range core {
		if !root[n] {
			missing = append(missing, n)
		}
	}
	for n := range root {
		if !core[n] {
			extra = append(extra, n)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 {
		t.Errorf("internal/core exports names the root aquifer.go doesn't re-export: %v", missing)
	}
	if len(extra) > 0 {
		t.Errorf("the root aquifer.go re-exports names internal/core no longer has: %v", extra)
	}
}

func exportedNames(t *testing.T, dir string, include func(string) bool) map[string]bool {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi os.FileInfo) bool { return include(fi.Name()) }, 0)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, p := range pkgs {
		for _, f := range p.Files {
			for _, d := range f.Decls {
				switch d := d.(type) {
				case *ast.FuncDecl:
					if d.Recv == nil && d.Name.IsExported() {
						names[d.Name.Name] = true
					}
				case *ast.GenDecl:
					for _, s := range d.Specs {
						switch s := s.(type) {
						case *ast.TypeSpec:
							if s.Name.IsExported() {
								names[s.Name.Name] = true
							}
						case *ast.ValueSpec:
							for _, n := range s.Names {
								if n.IsExported() {
									names[n.Name] = true
								}
							}
						}
					}
				}
			}
		}
	}
	return names
}
