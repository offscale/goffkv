package goffkv_test

import (
    goffkv "github.com/offscale/goffkv"
    "testing"
    "strings"
)

func stringSlicesEqual(a []string, b []string) bool {
    if len(a) != len(b) {
        return false
    }
    for i := 0; i < len(a); i++ {
        if a[i] != b[i] {
            return false
        }
    }
    return true
}

func dumbSplit(s string) []string {
    r := strings.Split(s, "/")
    if len(r) > 0 && r[0] == "" {
        r = r[1:]
    }
    return r
}

var (
    // "Strings" here means "both keys and paths". Note that neither valid nor invalid "strings"
    // should include the empty string; it is tested separately for keys and paths.
    validStrings = []string{
        "/zxcvbn",
        "/zxcvbn/1",
        "/parent/child",
        "/.../.../zookeper",
    }
    invalidStrings = []string{
        "/",
        "mykey",
        "/каша",
        "/test\n", "/test\t",
        "/zookeeper", "/zookeeper/child", "/zookeeper/..", "/one/zookeeper",
        "/one/two//three",
        "/one/two/three/",
        "/one/two/three/.",
        "/one/./three",
        "/one/../three",
        "/one/two/three/..",
    }
)

func TestDisassembleKey(t *testing.T) {
    for _, key := range validStrings {
        segments, err := goffkv.DisassembleKey(key)
        if err != nil {
            t.Fatalf("key %q: got error: %v", key, err)
        }
        expected := dumbSplit(key)
        if !stringSlicesEqual(segments, expected) {
            t.Fatalf("key %q: expected segments %v, found %v", key, expected, segments)
        }
    }

    invalidKeys := append(invalidStrings, "")
    for _, key := range invalidKeys {
        _, err := goffkv.DisassembleKey(key)
        _, ok := err.(goffkv.UsageError)
        if !ok {
            t.Fatalf("key %q: expected goffkv.UsageError error, found %v", key, err)
        }
    }
}

func TestDisassemblePath(t *testing.T) {
    validPaths := append(validStrings, "")
    for _, path := range validPaths {
        segments, err := goffkv.DisassemblePath(path)
        if err != nil {
            t.Fatalf("path %q: got error: %v", path, err)
        }
        expected := dumbSplit(path)
        if !stringSlicesEqual(segments, expected) {
            t.Fatalf("path %q: expected segments %v, found %v", path, expected, segments)
        }
    }

    for _, path := range invalidStrings {
        _, err := goffkv.DisassemblePath(path)
        _, ok := err.(goffkv.UsageError)
        if !ok {
            t.Fatalf("path %q: expected goffkv.UsageError error, found %v", path, err)
        }
    }
}
