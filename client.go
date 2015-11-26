package main

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"net/http"
	"sort"
)

type HashFunc []uint32

type KV struct {
	Key   int    `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

func (hf HashFunc) Len() int {
	return len(hf)
}

func (hf HashFunc) Less(i, j int) bool {
	return hf[i] < hf[j]
}

func (hf HashFunc) Swap(i, j int) {
	hf[i], hf[j] = hf[j], hf[i]
}

type Node struct {
	Id int
	IP string
}

func NewNode(id int, ip string) *Node {
	return &Node{
		Id: id,
		IP: ip,
	}
}

type HashC struct {
	Nodes     map[uint32]Node
	IsPresent map[int]bool
	HCircle   HashFunc
}

func NewHashC() *HashC {
	return &HashC{
		Nodes:     make(map[uint32]Node),
		IsPresent: make(map[int]bool),
		HCircle:   HashFunc{},
	}
}

func (hc *HashC) AddNode(node *Node) bool {

	if _, ok := hc.IsPresent[node.Id]; ok {
		return false
	}
	str := hc.ReturnNodeIP(node)
	hc.Nodes[hc.GetHashValue(str)] = *(node)
	hc.IsPresent[node.Id] = true
	hc.SortHashCircle()
	return true
}

func (hc *HashC) SortHashCircle() {
	hc.HCircle = HashFunc{}
	for k := range hc.Nodes {
		hc.HCircle = append(hc.HCircle, k)
	}
	sort.Sort(hc.HCircle)
}

func (hc *HashC) ReturnNodeIP(node *Node) string {
	return node.IP
}

func (hc *HashC) GetHashValue(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (hc *HashC) Get(key string) Node {
	hash := hc.GetHashValue(key)
	i := hc.SearchForNode(hash)
	return hc.Nodes[hc.HCircle[i]]
}

func (hc *HashC) SearchForNode(hash uint32) int {
	i := sort.Search(len(hc.HCircle), func(i int) bool { return hc.HCircle[i] >= hash })
	if i < len(hc.HCircle) {
		if i == len(hc.HCircle)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(hc.HCircle) - 1
	}
}

func PutKey(circleHash *HashC, str string, input string) {
	ipAddress := circleHash.Get(str)
	address := "http://" + ipAddress.IP + "/keys/" + str + "/" + input
	fmt.Println(address)
	req, err := http.NewRequest("PUT", address, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer resp.Body.Close()
		fmt.Println("PUT Request successfully completed")
	}
}

func GetAKey(key string, cHash *HashC) {
	var out KV
	ipAddress := cHash.Get(key)
	address := "http://" + ipAddress.IP + "/keys/" + key
	fmt.Println(address)
	response, err := http.Get(address)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println(err)
		}
		json.Unmarshal(contents, &out)
		result, _ := json.Marshal(out)
		fmt.Println(string(result))
	}
}

func GetAllKeys(address string) {

	var out []KV
	response, err := http.Get(address)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println(err)
		}
		json.Unmarshal(contents, &out)
		result, _ := json.Marshal(out)
		fmt.Println(string(result))
	}
}
func main() {
	circleHash := NewHashC()
	circleHash.AddNode(NewNode(0, "127.0.0.1:3000"))
	circleHash.AddNode(NewNode(1, "127.0.0.1:3001"))
	circleHash.AddNode(NewNode(2, "127.0.0.1:3002"))

	PutKey(circleHash, "1", "a")
	PutKey(circleHash, "2", "b")
	PutKey(circleHash, "3", "c")
	PutKey(circleHash, "4", "d")
	PutKey(circleHash, "5", "e")
	PutKey(circleHash, "6", "f")
	PutKey(circleHash, "7", "g")
	PutKey(circleHash, "8", "h")
	PutKey(circleHash, "9", "i")
	PutKey(circleHash, "10", "j")

	fmt.Println("==================")

	GetAKey("1", circleHash)
	GetAKey("2", circleHash)
	GetAKey("3", circleHash)
	GetAKey("4", circleHash)
	GetAKey("5", circleHash)
	GetAKey("6", circleHash)
	GetAKey("7", circleHash)
	GetAKey("8", circleHash)
	GetAKey("9", circleHash)
	GetAKey("10", circleHash)

	GetAllKeys("http://127.0.0.1:3000/keys")
	GetAllKeys("http://127.0.0.1:3001/keys")
	GetAllKeys("http://127.0.0.1:3002/keys")
}
