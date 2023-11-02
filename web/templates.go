package main

import (
	"bytes"
	"html/template"
	"path/filepath"
)

// Return initial map of data for HTML templates
func MakeTmplData() map[string]any {
	tmplData := make(map[string]any)
	tmplData["Base"] = Config.Base
	return tmplData
}

// Fill out a template and return it
func FormatTemplate(tdir, tfile string, tdata map[string]any) string {
	buf := new(bytes.Buffer)
	t := template.Must(template.New(tfile).ParseFiles(filepath.Join(tdir, tfile)))
	err := t.Execute(buf, tdata)
	if err != nil {
		panic(err)
	}
	return buf.String()
}
