package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"
)

type KV struct {
	Key   int    `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

var k1, k2, k3 []KV
var iOne, iTwo, iThree int

type Keywise []KV

func (a Keywise) Len() int           { return len(a) }
func (a Keywise) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Keywise) Less(i, j int) bool { return a[i].Key < a[j].Key }

func GetKeys(rw http.ResponseWriter, request *http.Request, p httprouter.Params) {
	port := strings.Split(request.Host, ":")
	if port[1] == "3000" {
		sort.Sort(Keywise(k1))
		resultOne, _ := json.Marshal(k1)
		fmt.Fprintln(rw, string(resultOne))
	} else if port[1] == "3001" {
		sort.Sort(Keywise(k2))
		resultTwo, _ := json.Marshal(k2)
		fmt.Fprintln(rw, string(resultTwo))
	} else {
		sort.Sort(Keywise(k3))
		resultThree, _ := json.Marshal(k3)
		fmt.Fprintln(rw, string(resultThree))
	}
}

func PutKey(rw http.ResponseWriter, request *http.Request, p httprouter.Params) {
	port := strings.Split(request.Host, ":")
	key, _ := strconv.Atoi(p.ByName("key_id"))
	if port[1] == "3000" {
		k1 = append(k1, KV{key, p.ByName("value")})
		iOne++
	} else if port[1] == "3001" {
		k2 = append(k2, KV{key, p.ByName("value")})
		iTwo++
	} else {
		k3 = append(k3, KV{key, p.ByName("value")})
		iThree++
	}
}

func GetAKey(rw http.ResponseWriter, request *http.Request, p httprouter.Params) {
	op := k1
	index := iOne
	port := strings.Split(request.Host, ":")
	if port[1] == "3001" {
		op = k2
		index = iTwo
	} else if port[1] == "3002" {
		op = k3
		index = iThree
	}
	key, _ := strconv.Atoi(p.ByName("key_id"))
	for i := 0; i < index; i++ {
		if op[i].Key == key {
			result, _ := json.Marshal(op[i])
			fmt.Fprintln(rw, string(result))
		}
	}
}

func main() {
	iOne = 0
	iTwo = 0
	iThree = 0
	mux := httprouter.New()
	mux.GET("/keys", GetKeys)
	mux.GET("/keys/:key_id", GetAKey)
	mux.PUT("/keys/:key_id/:value", PutKey)
	go http.ListenAndServe(":3000", mux)
	go http.ListenAndServe(":3001", mux)
	go http.ListenAndServe(":3002", mux)
	select {}
}
