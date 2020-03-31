package goffkv

import (
    "strings"
    "fmt"
)

// This operation could not possibly have successed: for example, a key or address are invalid.
type UsageError struct {
    msg string
    arg string
}

// This (non-transaction) operation could possibly have successed, but failed.
type OpError struct {
    msg string
}

// Transaction failed on some operation.
type TxnError struct {
    OpIndex int
}

var (
    OpErrNoEntry     = OpError{"no entry"}
    OpErrEntryExists = OpError{"entry exists"}
    OpErrEphem       = OpError{"attempt to create a child of ephemeral node"}
)

func (e UsageError) Error() string {
    return fmt.Sprintf("%s: %q", e.msg, e.arg)
}

func (e OpError) Error() string {
    return e.msg
}

func (e TxnError) Error() string {
    return fmt.Sprintf("transaction failed on operation with index %d", e.OpIndex)
}

type Version = uint64
type Watch = func()
type Action int

const (
    Create Action = iota + 1
    Set
    Erase
)

type Check struct {
    Key string
    Ver Version
}

type Operation struct {
    What Action
    Key string
    Value []byte
    Lease bool
}

type Txn struct {
    Checks []Check
    Ops []Operation
}

type TxnOpResult struct {
    What Action
    Ver Version
}

type Client interface {
    Create(key string, value []byte, lease bool)  (Version, error)
    Set(key string, value []byte)                 (Version, error)
    Cas(key string, value []byte, ver Version)    (Version, error)
    Erase(key string, ver Version)                error
    Exists(key string, watch bool)                (Version, Watch, error)
    Get(key string, watch bool)                   (Version, []byte, Watch, error)
    Children(key string, watch bool)              ([]string, Watch, error)
    Commit(txn Txn)                               ([]TxnOpResult, error)
    Close()
}

type NewClientFunc func(address string, prefix string) (Client, error)

var (
    clientRegistry = make(map[string]NewClientFunc)
)

func RegisterClient(scheme string, newFunc NewClientFunc) {
    clientRegistry[scheme] = newFunc
}

func Open(url string, prefix string) (Client, error) {
    parts := strings.SplitN(url, "://", 2)
    if len(parts) != 2 {
        return nil, UsageError{msg: "invalid URL", arg: url}
    }
    scheme, address := parts[0], parts[1]

    newFunc, ok := clientRegistry[scheme]
    if !ok {
        return nil, UsageError{msg: "unknown scheme", arg: scheme}
    }
    return newFunc(address, prefix)
}
