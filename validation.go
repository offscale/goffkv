package goffkv

import "strings"

func checkSegment(segment string) bool {
    for _, c := range segment {
        if c <= 0x1F || c >= 0x7F {
            return false
        }
    }
    return segment != "" && segment != "." && segment != ".." && segment != "zookeeper"
}

func DisassemblePath(path string) ([]string, error) {
    if path == "" {
        return []string{}, nil
    }
    if path[0] != '/' {
        return nil, UsageError{msg: "invalid path", arg: path}
    }
    segments := strings.Split(path[1:], "/")
    for _, segment := range segments {
        if !checkSegment(segment) {
            return nil, UsageError{msg: "invalid path", arg: path}
        }
    }
    return segments, nil
}

func DisassembleKey(key string) ([]string, error) {
    segments, err := DisassemblePath(key)
    if err != nil {
        return nil, err
    }
    if len(segments) == 0 {
        return nil, UsageError{msg: "invalid key", arg: key}
    }
    return segments, nil
}
