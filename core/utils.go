package core

import (
	"crypto/sha1"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
)

func Perr(e error) {
  perr(e)
}

func perr(e error) {
  if e != nil {
    panic(e)
  }
}

func HashString(s string) string {
  h := sha1.New()
  h.Write([]byte(s))
  r := h.Sum(nil)
  return hex.EncodeToString(r)
}

func HashFile(p string) string {
  data, e := os.ReadFile(p)
  perr(e)
  sdata := string(data)
  return HashString(sdata) 
}


func DoesPathExist(path string) bool {
  _, err := os.Stat(path)

  if os.IsNotExist(err) {
    return false
  }

  return true
}

func ListAllFilesInPath(path string) []string {
  var files []string
  err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
    if err != nil {
      return err
    }

    if !info.IsDir() && strings.HasSuffix(path, ".sql") {
      files = append(files, path)
    }

    return nil 
  })

  perr(err)
  return files
}
