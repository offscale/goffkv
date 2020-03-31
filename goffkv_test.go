package goffkv_test

import (
    goffkv "github.com/offscale/goffkv"
    _ "github.com/offscale/goffkv-consul"
    _ "github.com/offscale/goffkv-zk"
    _ "github.com/offscale/goffkv-etcd"
    "testing"
    "fmt"
    "bytes"
    "time"
)

type keyHolder struct {
    keys []string
    client goffkv.Client
}

func holdKeys(client goffkv.Client, keys ...string) keyHolder {
    for _, key := range keys {
        _ = client.Erase(key, 0)
    }
    return keyHolder{keys, client}
}

func (kh keyHolder) cleanup() {
    for _, key := range kh.keys {
        _ = kh.client.Erase(key, 0)
    }
}

func generateData() []byte {
    return []byte("wpqznrhankjpjivpwxixcqkfmpumwyqs")
}

func testCreateExists(t *testing.T, client goffkv.Client) {
    kh := holdKeys(client, "/test")
    defer kh.cleanup()

    value := generateData()

    ver, watch, err := client.Exists("/test", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver != 0 {
        t.Fatalf("expected no version (0), found %v", ver)
    }

    ver, err = client.Create("/test", value, false)
    if err != nil {
        t.Fatal(err)
    }

    ver2, watch, err := client.Exists("/test", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver2 != ver {
        t.Fatalf("expected version %v, found %v", ver, ver2)
    }
}

func testCreateGet(t *testing.T, client goffkv.Client) {
    kh := holdKeys(client, "/test")
    defer kh.cleanup()

    value := generateData()

    _, _, _, err := client.Get("/test", false)
    if err != goffkv.OpErrNoEntry {
        t.Fatalf("expected goffkv.OpErrNoEntry error, found %v", err)
    }

    ver, err := client.Create("/test", value, false)
    if err != nil {
        t.Fatal(err)
    }

    ver2, value2, watch, err := client.Get("/test", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver2 != ver {
        t.Fatalf("expected version %v, found %v", ver, ver2)
    }
    if !bytes.Equal(value2, value) {
        t.Fatalf("expected value %v, found %v", value, value2)
    }
}

func testCreate(t *testing.T, client goffkv.Client) {
    kh := holdKeys(client, "/key")
    defer kh.cleanup()

    value := generateData()

    _, err := client.Create("/key", value, false)
    if err != nil {
        t.Fatal(err)
    }

    _, err = client.Create("/key", value, false)
    if err != goffkv.OpErrEntryExists {
        t.Fatalf("expected goffkv.OpErrEntryExists error, found %v", err)
    }

    _, err = client.Create("/key/child/grandchild", value, false)
    if err != goffkv.OpErrNoEntry {
        t.Fatalf("expected goffkv.OpErrNoEntry error, found %v", err)
    }

    _, err = client.Create("/key/child", value, false)
    if err != nil {
        t.Fatal(err)
    }
}

func testEraseNoKey(t *testing.T, client goffkv.Client) {
    kh := holdKeys(client, "/key")
    defer kh.cleanup()

    err := client.Erase("/key", 0)
    if err != goffkv.OpErrNoEntry {
        t.Fatalf("expected goffkv.OpErrNoEntry error, found %v", err)
    }
}

func testEraseExists(t *testing.T, client goffkv.Client) {
    kh := holdKeys(client, "/key")
    defer kh.cleanup()

    value := generateData()

    _, err := client.Create("/key", value, false)
    if err != nil {
        t.Fatal(err)
    }

    _, err = client.Create("/key/child", value, false)
    if err != nil {
        t.Fatal(err)
    }

    err = client.Erase("/key", 0)
    if err != nil {
        t.Fatal(err)
    }

    ver, watch, err := client.Exists("/key", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver != 0 {
        t.Fatalf("expected no version (0), found %v", ver)
    }

    ver, watch, err = client.Exists("/key/child", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver != 0 {
        t.Fatalf("expected no version (0), found %v", ver)
    }
}

func testEraseVersioned(t *testing.T, client goffkv.Client) {
    kh := holdKeys(client, "/key")
    defer kh.cleanup()

    value := generateData()

    ver, err := client.Create("/key", value, false)
    if err != nil {
        t.Fatal(err)
    }

    err = client.Erase("/key", ver + 1)
    if err != nil {
        t.Fatal(err)
    }

    ver2, watch, err := client.Exists("/key", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver2 != ver {
        t.Fatalf("expected version %v, found %v", ver, ver2)
    }

    err = client.Erase("/key", ver)
    if err != nil {
        t.Fatal(err)
    }

    ver2, watch, err = client.Exists("/key", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver2 != 0 {
        t.Fatalf("expected no version (0), found %v", ver2)
    }
}

func testSetGet(t *testing.T, client goffkv.Client) {
    kh := holdKeys(client, "/key")
    defer kh.cleanup()

    value1 := []byte("bvdvqdbuujcyynjeuxywzzqsnjvliyua")
    value2 := []byte("ltljebrisknzmnprimnybqagdqmzasbg")

    ver1, err := client.Create("/key", value1, false)
    if err != nil {
        t.Fatal(err)
    }

    ver2, err := client.Set("/key", value2)
    if err != nil {
        t.Fatal(err)
    }
    if ver2 <= ver1 {
        t.Fatalf("new version (%v) is not greater than old (%v)", ver2, ver1)
    }

    ver3, value3, watch, err := client.Get("/key", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver3 != ver2 {
        t.Fatalf("expected version %v, found %v", ver2, ver3)
    }
    if !bytes.Equal(value2, value3) {
        t.Fatalf("expected value %v, found %v", value2, value3)
    }
}

func testSetGetHot(t *testing.T, client goffkv.Client) {
    kh := holdKeys(client, "/key")
    defer kh.cleanup()

    value := generateData()

    _, err := client.Set("/key/child", value)
    if err != goffkv.OpErrNoEntry {
        t.Fatalf("expected goffkv.OpErrNoEntry error, found %v", err)
    }

    _, err = client.Set("/key/child/grandchild", value)
    if err != goffkv.OpErrNoEntry {
        t.Fatalf("expected goffkv.OpErrNoEntry error, found %v", err)
    }

    _, err = client.Set("/key", value)
    if err != nil {
        t.Fatal(err)
    }

    _, value2, watch, err := client.Get("/key", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if !bytes.Equal(value2, value) {
        t.Fatalf("expected value %v, found %v", value, value2)
    }
}

func testChildrenNoKey(t *testing.T, client goffkv.Client) {
    kh := holdKeys(client, "/key")
    defer kh.cleanup()

    _, _, err := client.Children("/key", false)
    if err != goffkv.OpErrNoEntry {
        t.Fatalf("expected goffkv.OpErrNoEntry error, found %v", err)
    }
}

func stringSetsEqual(a []string, b []string) bool {
    m := make(map[string]int)
    for _, s := range a {
        m[s]++
    }
    for _, s := range b {
        m[s]--
    }
    for _, v := range m {
        if v != 0 {
            return false
        }
    }
    return true
}

func testChildren(t *testing.T, client goffkv.Client) {
    kh := holdKeys(client, "/key")
    defer kh.cleanup()

    value := generateData()

    keys := []string{
        "/key",
        "/key/child",
        "/key/child/grandchild",
        "/key/hi",
    }
    for _, key := range keys {
        _, err := client.Create(key, value, false)
        if err != nil {
            t.Fatalf("cannot create key %v: %v", key, err)
        }
    }

    result, watch, err := client.Children("/key", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }

    expected := []string{"/key/child", "/key/hi"}
    if !stringSetsEqual(expected, result) {
        t.Fatalf("expected children list %v, found %v", expected, result)
    }

    result, watch, err = client.Children("/key/child", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }

    expected = []string{"/key/child/grandchild"}
    if !stringSetsEqual(expected, result) {
        t.Fatalf("expected children list %v, found %v", expected, result)
    }
}

func testCasNoKey(t *testing.T, client goffkv.Client) {
    kh := holdKeys(client, "/key")
    defer kh.cleanup()

    value := generateData()

    _, err := client.Cas("/key", value, 42)
    if err != goffkv.OpErrNoEntry {
        t.Fatalf("expected goffkv.OpErrNoEntry error, found %v", err)
    }
}

func testCas(t *testing.T, client goffkv.Client) {
    kh := holdKeys(client, "/key")
    defer kh.cleanup()

    value1 := []byte("bvdvqdbuujcyynjeuxywzzqsnjvliyua")
    value2 := []byte("ltljebrisknzmnprimnybqagdqmzasbg")

    ver, err := client.Create("/key", value1, false)
    if err != nil {
        t.Fatal(err)
    }

    ver2, err := client.Cas("/key", value2, ver + 1)
    if err != nil {
        t.Fatal(err)
    }
    if ver2 != 0 {
        t.Fatalf("expected no version (0), found %v", ver2)
    }

    ver3, value3, watch, err := client.Get("/key", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver3 != ver {
        t.Fatalf("expected version %v, found %v", ver, ver3)
    }
    if !bytes.Equal(value3, value1) {
        t.Fatalf("expected value %v, found %v", value2, value1)
    }

    ver4, err := client.Cas("/key", value2, ver)
    if err != nil {
        t.Fatal(err)
    }
    if ver4 <= ver {
        t.Fatalf("new version (%v) is not greater than old (%v)", ver4, ver)
    }

    ver5, value5, watch, err := client.Get("/key", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver5 != ver4 {
        t.Fatalf("expected version %v, found %v", ver4, ver5)
    }
    if !bytes.Equal(value5, value2) {
        t.Fatalf("expected value %v, found %v", value2, value5)
    }
}

func testCasZeroVersion(t *testing.T, client goffkv.Client) {
    kh := holdKeys(client, "/key")
    defer kh.cleanup()

    value1 := []byte("bvdvqdbuujcyynjeuxywzzqsnjvliyua")
    value2 := []byte("ltljebrisknzmnprimnybqagdqmzasbg")

    ver1, err := client.Cas("/key", value1, 0)
    if err != nil {
        t.Fatal(err)
    }
    if ver1 == 0 {
        t.Fatalf("expected non-zero version")
    }

    ver3, value3, watch, err := client.Get("/key", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver3 != ver1 {
        t.Fatalf("expected version %v, found %v", ver1, ver3)
    }
    if !bytes.Equal(value3, value1) {
        t.Fatalf("expected value %v, found %v", value1, value3)
    }

    ver4, err := client.Cas("/key", value2, 0)
    if err != nil {
        t.Fatal(err)
    }
    if ver4 != 0 {
        t.Fatalf("expected no version (0), found %v", ver4)
    }

    ver5, value5, watch, err := client.Get("/key", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver5 != ver1 {
        t.Fatalf("expected version %v, found %v", ver1, ver5)
    }
    if !bytes.Equal(value5, value1) {
        t.Fatalf("expected value %v, found %v", value1, value5)
    }
}

func testTxnSuccess(t *testing.T, client goffkv.Client) {
    kh := holdKeys(client, "/foo")
    defer kh.cleanup()

    value1 := []byte("bvdvqdbuujcyynjeuxywzzqsnjvliyua")
    value2 := []byte("ltljebrisknzmnprimnybqagdqmzasbg")
    value3 := []byte("wcsymfouyanlzcpeorctcczblrezecov")

    ver1, err := client.Create("/foo", value1, false)
    if err != nil {
        t.Fatal(err)
    }

    ver2, err := client.Create("/foo/bar", value2, false)
    if err != nil {
        t.Fatal(err)
    }

    result, err := client.Commit(goffkv.Txn{
        Checks: []goffkv.Check{
            goffkv.Check{"/foo", ver1},
            goffkv.Check{"/foo/bar", ver2},
        },
        Ops: []goffkv.Operation{
            goffkv.Operation{
                What: goffkv.Create,
                Key: "/foo/child",
                Value: value3,
                Lease: false,
            },
            goffkv.Operation{
                What: goffkv.Erase,
                Key: "/foo/bar",
            },
        },
    })
    if err != nil {
        t.Fatal(err)
    }
    if len(result) != 1 {
        t.Fatalf("expected result of length 1, got %v", result)
    }
    if result[0].What != goffkv.Create {
        t.Fatalf("expected result[0].What == goffkv.Create, found %v", result[0].What)
    }

    ver4, value4, watch, err := client.Get("/foo/child", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver4 != result[0].Ver {
        t.Fatalf("version mismatch: txn result was %v, get result is %v", result[0].Ver, ver4)
    }
    if !bytes.Equal(value4, value3) {
        t.Fatalf("value mismatch: txn set %v, get result is %v", value3, value4)
    }

    ver5, watch, err := client.Exists("/foo/bar", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver5 != 0 {
        t.Fatalf("expected no version (0), found %v", ver5)
    }
}

func testTxnFailureCheck(t *testing.T, client goffkv.Client) {
    kh := holdKeys(client, "/key", "/foo")
    defer kh.cleanup()

    value1 := []byte("bvdvqdbuujcyynjeuxywzzqsnjvliyua")
    value2 := []byte("ltljebrisknzmnprimnybqagdqmzasbg")
    value3 := []byte("wcsymfouyanlzcpeorctcczblrezecov")

    ver1, err := client.Create("/key", value1, false)
    if err != nil {
        t.Fatal(err)
    }

    ver2, err := client.Create("/foo", value2, false)
    if err != nil {
        t.Fatal(err)
    }

    ver3, err := client.Create("/foo/bar", value3, false)
    if err != nil {
        t.Fatal(err)
    }

    _, err = client.Commit(goffkv.Txn{
        Checks: []goffkv.Check{
            goffkv.Check{"/key", ver1},
            goffkv.Check{"/foo", ver2 + 1},
            goffkv.Check{"/foo/bar", ver3},
        },
        Ops: []goffkv.Operation{
            goffkv.Operation{
                What: goffkv.Create,
                Key: "/key/child",
                Value: value1,
                Lease: false,
            },
            goffkv.Operation{
                What: goffkv.Set,
                Key: "/key",
                Value: value2,
            },
            goffkv.Operation{
                What: goffkv.Erase,
                Key: "/foo",
            },
        },
    })

    txnerr, ok := err.(goffkv.TxnError)
    if !ok {
        t.Fatalf("expected goffkv.TxnError error, found %v", err)
    }

    if txnerr.OpIndex != 1 {
        t.Fatalf("expected txnerr.OpIndex == 1, found %v", txnerr.OpIndex)
    }

    ver4, watch, err := client.Exists("/key/child", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver4 != 0 {
        t.Fatalf("expected no version (0), found %v", ver4)
    }

    ver5, watch, err := client.Exists("/foo", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver5 == 0 {
        t.Fatalf("expected non-zero version")
    }

    _, value6, watch, err := client.Get("/foo", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if !bytes.Equal(value6, value2) {
        t.Fatalf("expected value %v, found %v", value2, value6)
    }
}

func testTxnFailureOp(t *testing.T, client goffkv.Client) {
    kh := holdKeys(client, "/key", "/foo")
    defer kh.cleanup()

    value1 := []byte("bvdvqdbuujcyynjeuxywzzqsnjvliyua")
    value2 := []byte("ltljebrisknzmnprimnybqagdqmzasbg")
    value3 := []byte("wcsymfouyanlzcpeorctcczblrezecov")

    ver1, err := client.Create("/key", value1, false)
    if err != nil {
        t.Fatal(err)
    }

    ver2, err := client.Create("/foo", value2, false)
    if err != nil {
        t.Fatal(err)
    }

    ver3, err := client.Create("/foo/bar", value3, false)
    if err != nil {
        t.Fatal(err)
    }

    _, err = client.Commit(goffkv.Txn{
        Checks: []goffkv.Check{
            goffkv.Check{"/key", ver1},
            goffkv.Check{"/foo", ver2},
            goffkv.Check{"/foo/bar", ver3},
        },
        Ops: []goffkv.Operation{
            goffkv.Operation{
                What: goffkv.Create,
                Key: "/key/child",
                Value: value1,
                Lease: false,
            },
            goffkv.Operation{
                What: goffkv.Set,
                Key: "/key/child2/grandchild",
                Value: value2,
            },
            goffkv.Operation{
                What: goffkv.Erase,
                Key: "/foo",
            },
        },
    })

    txnerr, ok := err.(goffkv.TxnError)
    if !ok {
        t.Fatalf("expected goffkv.TxnError error, found %v", err)
    }

    if txnerr.OpIndex != 4 {
        t.Fatalf("expected txnerr.OpIndex == 4, found %v", txnerr.OpIndex)
    }

    ver4, watch, err := client.Exists("/key/child", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver4 != 0 {
        t.Fatalf("expected no version (0), found %v", ver4)
    }

    ver5, watch, err := client.Exists("/foo", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if ver5 == 0 {
        t.Fatalf("expected non-zero version")
    }

    _, value6, watch, err := client.Get("/foo", false)
    if err != nil {
        t.Fatal(err)
    }
    if watch != nil {
        t.Fatalf("expected nil watch")
    }
    if !bytes.Equal(value6, value2) {
        t.Fatalf("expected value %v, found %v", value2, value6)
    }
}

const (
    maxLag = time.Second
    watchUsefulCheckTimeout = time.Second * 2
)

func testWaitSignal(t *testing.T, wait func(), signal func(), checkUseful bool) {
    c := make(chan struct{})

    go func() {
        wait()
        close(c)
    }()

    if checkUseful {
        select {
        case <-c:
            t.Fatalf("wait() returned before actual signal")
        case <-time.After(watchUsefulCheckTimeout):
        }
    }
    signal()
    select {
    case <-time.After(maxLag):
        t.Fatalf("timeout reached before wait() returned")
    case <-c:
    }
}

func testWatchExists(t *testing.T, client goffkv.Client, checkUseful bool) {
    kh := holdKeys(client, "/key")
    defer kh.cleanup()

    value := generateData()

    _, err := client.Create("/key", value, false)
    if err != nil {
        t.Fatal(err)
    }

    ver, watch, err := client.Exists("/key", true)
    if err != nil {
        t.Fatal(err)
    }
    if ver == 0 {
        t.Fatalf("expected non-zero version")
    }

    testWaitSignal(t, watch, func() {
        err := client.Erase("/key", 0)
        if err != nil {
            t.Fatal(err)
        }
    }, checkUseful)

    ver, _, err = client.Exists("/key", false)
    if err != nil {
        t.Fatal(err)
    }
    if ver != 0 {
        t.Fatalf("expected no version (0), found %v", ver)
    }
}

func testWatchGet(t *testing.T, client goffkv.Client, checkUseful bool) {
    kh := holdKeys(client, "/key")
    defer kh.cleanup()

    value1 := []byte("bvdvqdbuujcyynjeuxywzzqsnjvliyua")
    value2 := []byte("ltljebrisknzmnprimnybqagdqmzasbg")

    _, err := client.Create("/key", value1, false)
    if err != nil {
        t.Fatal(err)
    }

    _, _, watch, err := client.Get("/key", true)
    if err != nil {
        t.Fatal(err)
    }

    testWaitSignal(t, watch, func() {
        _, err := client.Set("/key", value2)
        if err != nil {
            t.Fatal(err)
        }
    }, checkUseful)

    _, value3, _, err := client.Get("/key", false)
    if err != nil {
        t.Fatal(err)
    }
    if !bytes.Equal(value3, value2) {
        t.Fatalf("expected value %v, found %v", value2, value3)
    }
}

func testWatchChildren(t *testing.T, client goffkv.Client, checkUseful bool) {
    kh := holdKeys(client, "/key")
    defer kh.cleanup()

    value := generateData()

    keys := []string{
        "/key",
        "/key/child",
        "/key/child/grandchild",
        "/key/hi",
    }
    for _, key := range keys {
        _, err := client.Create(key, value, false)
        if err != nil {
            t.Fatalf("cannot create key %v: %v", key, err)
        }
    }

    result, watch, err := client.Children("/key", true)
    if err != nil {
        t.Fatal(err)
    }
    expected := []string{"/key/child", "/key/hi"}
    if !stringSetsEqual(result, expected) {
        t.Fatalf("expected children set %v, found %v", expected, result)
    }

    testWaitSignal(t, watch, func() {
        err := client.Erase("/key/hi", 0)
        if err != nil {
            t.Fatal(err)
        }
    }, checkUseful)

    result, _, err = client.Children("/key", false)
    if err != nil {
        t.Fatal(err)
    }
    expected = []string{"/key/child"}
    if !stringSetsEqual(result, expected) {
        t.Fatalf("expected children set %v, found %v", expected, result)
    }
}

func runTestsUrlPrefix(t *testing.T, url string, prefix string) {
    client, err := goffkv.Open(url, prefix)
    if err != nil {
        t.Fatal(err)
    }
    defer client.Close()

    t.Run("create_exists", func(t *testing.T) {
        testCreateExists(t, client)
    })

    t.Run("create_get", func(t *testing.T) {
        testCreateGet(t, client)
    })

    t.Run("create", func(t *testing.T) {
        testCreate(t, client)
    })

    t.Run("erase_no_key", func(t *testing.T) {
        testEraseNoKey(t, client)
    })

    t.Run("erase_exists", func(t *testing.T) {
        testEraseExists(t, client)
    })

    t.Run("erase_versioned", func(t *testing.T) {
        testEraseVersioned(t, client)
    })

    t.Run("set_get", func(t *testing.T) {
        testSetGet(t, client)
    })

    t.Run("set_get_hot", func(t *testing.T) {
        testSetGetHot(t, client)
    })

    t.Run("children_no_key", func(t *testing.T) {
        testChildrenNoKey(t, client)
    })

    t.Run("children", func(t *testing.T) {
        testChildren(t, client)
    })

    t.Run("cas_no_key", func(t *testing.T) {
        testCasNoKey(t, client)
    })

    t.Run("cas", func(t *testing.T) {
        testCas(t, client)
    })

    t.Run("cas_zero_version", func(t *testing.T) {
        testCasZeroVersion(t, client)
    })

    t.Run("txn_success", func(t *testing.T) {
        testTxnSuccess(t, client)
    })

    t.Run("txn_failure_check", func(t *testing.T) {
        testTxnFailureCheck(t, client)
    })

    t.Run("txn_failure_op", func(t *testing.T) {
        testTxnFailureOp(t, client)
    })

    t.Run("watch_exists", func(t *testing.T) {
        testWatchExists(t, client, false)
    })

    t.Run("watch_exists_useful", func(t *testing.T) {
        testWatchExists(t, client, true)
    })

    t.Run("watch_get", func(t *testing.T) {
        testWatchGet(t, client, false)
    })

    t.Run("watch_get_useful", func(t *testing.T) {
        testWatchGet(t, client, true)
    })

    t.Run("watch_children", func(t *testing.T) {
        testWatchChildren(t, client, false)
    })

    t.Run("watch_children_useful", func(t *testing.T) {
        testWatchChildren(t, client, true)
    })

    t.Run("create_leased", func(t *testing.T) {
        kh := holdKeys(client, "/key")
        defer kh.cleanup()

        client2, err := goffkv.Open(url, prefix)
        if err != nil {
            t.Fatal(err)
        }

        value := generateData()

        _, err = client2.Create("/key", value, true)
        if err != nil {
            t.Fatal(err)
        }

        ver, _, err := client.Exists("/key", false)
        if err != nil {
            t.Fatal(err)
        }
        if ver == 0 {
            t.Fatalf("expected non-zero version")
        }

        client2.Close()

        time.Sleep(10 * time.Second + maxLag)
        ver, _, err = client.Exists("/key", false)
        if err != nil {
            t.Fatal(err)
        }
        if ver != 0 {
            t.Fatalf("expected no version (0), found %v", ver)
        }
    })
}

func runTestsUrl(t *testing.T, url string) {
    prefixes := []string{
        "",
        "/single-segment",
        "/two/segment",
        "/three/segment/prefix",
        "/q/u/i/t/e/ /a/ /c/o/m/p/l/e/x /p/r/e/f/i/x",
    }
    for i, prefix := range prefixes {
        prefix := prefix
        t.Run(
            fmt.Sprintf("prefix=%d", i),
            func(t *testing.T) {
                runTestsUrlPrefix(t, url, prefix)
            })
    }
}

func TestConsul(t *testing.T) {
    runTestsUrl(t, "consul://localhost:8500")
}

func TestZk(t *testing.T) {
    runTestsUrl(t, "zk://localhost:2181")
}

func TestEtcd(t *testing.T) {
    runTestsUrl(t, "etcd://localhost:2379")
}

func expectUsageError(t *testing.T, url string, prefix string) {
    _, err := goffkv.Open(url, prefix)
    _, ok := err.(goffkv.UsageError)
    if !ok {
        t.Fatalf("expected goffkv.UsageError error from goffkv.Open(%q, %q), found %v",
                 url, prefix, err)
    }
}

func TestOpen(t *testing.T) {
    expectUsageError(t, "consul://localhost:0", "invalidprefix")
    expectUsageError(t, "wrong://localhost:8500", "")
}
