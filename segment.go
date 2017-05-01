package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"
	"sync"
	"hash/fnv"
	"math"
	"math/rand"
)

var wormgatePort string
var segmentPort string

var hostname string

var targetSegments int32

var segmentClient *http.Client

var targetlist []string
var alivelist []string

var ping int32

var maxRunTime time.Duration

type Lottery struct {
	mux sync.Mutex
	lottery []int
}

var ticket uint32
var rticket uint32
var ticketlist []uint32
var winner uint32

var src = rand.NewSource(time.Now().UnixNano())

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

const (
	letterIdxBits = 6			//6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1	//All 1-bits, as many as letterIdxBits
	letterIdxMax = 63 / letterIdxBits 	// # of letter indices fitting in 63 bits
)

func RandString(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMax); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)

}


func main() {

	hostname, _ = os.Hostname()
	strip := strings.Split(hostname, ".local")
	hn := strip[0]
	ticket = hash(hn)
	log.SetPrefix(hostname + " segment: ")

	var spreadMode = flag.NewFlagSet("spread", flag.ExitOnError)
	addCommonFlags(spreadMode)
	var spreadHost = spreadMode.String("host", "localhost", "host to spread to")

	var runMode = flag.NewFlagSet("run", flag.ExitOnError)
	addCommonFlags(runMode)

	if len(os.Args) == 1 {
		log.Fatalf("No mode specified\n")
	}

	switch os.Args[1] {
	case "spread":
		spreadMode.Parse(os.Args[2:])
		sendSegment(*spreadHost)
	case "run":
		runMode.Parse(os.Args[2:])
		startSegmentServer()

	default:
		log.Fatalf("Unknown mode %q\n", os.Args[1])
	}
}

func addCommonFlags(flagset *flag.FlagSet) {
	flagset.StringVar(&wormgatePort, "wp", ":8181", "wormgate port (prefix with colon)")
	flagset.StringVar(&segmentPort, "sp", ":8182", "segment port (prefix with colon)")
	flagset.DurationVar(&maxRunTime, "maxrun", time.Minute*10, "max time to run(in case you forget to shut down)")
}


func sendSegment(address string) {

	url := fmt.Sprintf("http://%s%s/wormgate?sp=%s", address, wormgatePort, segmentPort)
	filename := "tmp.tar.gz"

	log.Printf("Spreading to %s", url)

	// ship the binary and the qml file that describes our screen output
	tarCmd := exec.Command("tar", "-zc", "-f", filename, "segment")
	tarCmd.Run()
	defer os.Remove(filename)

	file, err := os.Open(filename)
	if err != nil {
		log.Panic("Could not read input file", err)
	}

	resp, err := http.Post(url, "string", file)
	if err != nil {
		log.Panic("POST error ", err)
	}

	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode == 200 {
		log.Println("Received OK from server")
	} else {
		log.Println("Response: ", resp)
	}
}

func createClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{},
	}
}



func httpGetOk(client *http.Client, url string) (bool, string, error) {
	resp, err := client.Get(url)
	isOk := err == nil && resp.StatusCode == 200
	body := ""
	if err != nil {
		if strings.Contains(fmt.Sprint(err), "connection refused") {
			// ignore connection refused errors
			err = nil
		} else {
			log.Printf("Error checking %s: %s", url, err)
		}
	} else {
		var bytes []byte
		bytes, err = ioutil.ReadAll(resp.Body)
		body = string(bytes)
		resp.Body.Close()
	}
	return isOk, body, err
}


func doBcastPost(node string) error {
	url := fmt.Sprintf("http://%s%s/sync", node, segmentPort)
	postBody := strings.NewReader(fmt.Sprint(targetSegments))

	resp, err := segmentClient.Post(url, "text/plain", postBody)
	if err != nil && !strings.Contains(fmt.Sprint(err), "refused") {
		log.Printf("Error sync %s: %s", node, err)
	}
	if err == nil {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
	return err

}

func doBcastTicket(node string) error {
	random := RandString(10)
	rticket = hash(random)
	url := fmt.Sprintf("http://%s%s/ticket", node, segmentPort)
	postBody := strings.NewReader(fmt.Sprint(rticket))

	resp, err := segmentClient.Post(url, "text/plain", postBody)
	if err != nil && !strings.Contains(fmt.Sprint(err), "refused") {
		log.Printf("Error ticket %s: %s", node, err)
	}
	if err == nil {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
	return err
}

func doWormShutdownPost(node string) error {
	log.Printf("Posting shutdown to %s", node)

	url := fmt.Sprintf("http://%s%s/shutdown", node, segmentPort)

	resp, err := segmentClient.PostForm(url, nil)
	if err != nil && !strings.Contains(fmt.Sprint(err), "refused") {
		log.Printf("Error posting targetSegments %s: %s", node, err)
	}
	if err == nil {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
	return err
}

func syncHandler(w http.ResponseWriter, r *http.Request) {
	var ts int32

	pc, rateErr := fmt.Fscanf(r.Body, "%d", &ts)
	if pc != 1 || rateErr != nil {
		log.Printf("Error parsing synctarseg (%d items): %s", pc, rateErr)
	}

	atomic.StoreInt32(&targetSegments, ts)
	// Consume and close body
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	// Shut down
	log.Printf("Received sync command")
}

func calculate_diff(a uint32, b uint32) uint32{
	f := float64(a)
	f2 := float64(b)

	diff := f - f2
	abs := math.Abs(diff)

	return uint32(abs)
}

func find_winner() {
	//Check if ur the winner or loser
	var lowest uint32
	lowest = 0

	for _, num := range ticketlist {
		//log.Printf("Enter")
		diff := calculate_diff(num, rticket)

		if lowest == 0 {
			lowest = diff
			winner = num
			//log.Printf("winner1 %d", winner)
		} else {
			if lowest > diff {
				//log.Printf("winner2 %d", winner)
				lowest = diff
				winner = num
			}
		}
	}
	if winner == ticket {
		if ping < targetSegments {
			spawn_seg()
		} else if ping > targetSegments {
			//kill seg
			//log.Printf("Find winner kill hb %d, ts %d", ping, targetSegments)
			kill_seg()
		}
	}

}

func lotteryHandler(w http.ResponseWriter, r *http.Request) {
	var t uint32

	pc, rateErr := fmt.Fscanf(r.Body, "%d", &t)
	if pc != 1 || rateErr != nil {
		log.Printf("Error parsing lotteryticket (%d items): %s", pc, rateErr)
	}

	atomic.StoreUint32(&rticket, t)
	// Consume and close body
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	find_winner()
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func remove(slice []string, s string) {
	for i, a := range slice {
		if s == a {
			targetlist = append(slice[:i], slice[i+1:]...)
		}
	}
}

func remove3(slice []string, s string) {
	for i, a := range slice {
		if s == a {
			alivelist = append(slice[:i], slice[i+1:]...)
		}
	}
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()

}

func heartbeat() {
	segmentClient = createClient()
	list := fetchReachableHosts()
	targetlist = fetchReachableHosts()

	for {
		ping = 0
		for _, addr := range list {
			if addr == hostname || addr+".local" == hostname {
				ping++
				if contains(alivelist, addr) == false {
					//if hostname == addr+".local" {
						//strip := strings.Split(addr, ".local")
						//addr = strip[0]

					//}
					tmp := hash(addr)
					ticketlist = append(ticketlist, tmp)
					alivelist = append(alivelist, addr)
				}
				if contains(targetlist, addr) == true {
					remove(targetlist, addr)
					//log.Printf("%s remove %s", hostname, addr)
					//log.Printf("Targetlist %s", targetlist[:5])
				}

			} else {
				segmentUrl := fmt.Sprintf("http://%s%s/", addr, segmentPort)
				segment, _, _ := httpGetOk(segmentClient, segmentUrl)
				//segment, segBody, segErr := httpGetOk(segmentClient, segmentUrl)

				if segment == true {
					//log.Printf("\nAlive on addr: %s\n", addr)
					//targetlist = append(list[:i], list[i+1:]...)
					ping++
					if contains(alivelist, addr) == false {
						tmp := hash(addr)
						ticketlist = append(ticketlist, tmp)
						alivelist = append(alivelist, addr)
					}
					if contains(targetlist, addr) == true {
						remove(targetlist, addr)
						//log.Printf("%s remove %s", hostname, addr)
						//log.Printf("Targetlist %s", targetlist[:5])
					}
				} else {
					if contains(alivelist, addr) == true {
						remove3(alivelist, addr)
						targetlist = append(targetlist, addr)
					}
				}

			//if segErr != nil {
				//ping++
			//}
			}
		}
		//synce ogsa gjore lotteri
		if len(alivelist) > 1 && targetSegments > 1 {
			for _, addr := range alivelist {
				if addr != hostname || addr+".local" != hostname {
					//Sync targseg
					doBcastPost(addr)
					//time.Sleep(500 * time.Millisecond)

				}
			}
			//start lottery fordi alle har fatt sync
			for _, addr := range alivelist {
				if addr != hostname || addr+".local" != hostname {
					//Start lottery
					doBcastTicket(addr)
					//time.Sleep(500 * time.Millisecond)
					find_winner()
					//time.Sleep(500 * time.Millisecond)
				}
			}
			if winner == ticket {
				if ping < targetSegments {
					spawn_seg()
				} else if ping > targetSegments {
					//kill seg
					//log.Printf("Heartbitkill")
					kill_seg()
				}
			}

		}
		//}else {
			////Im alone
			//if ping < targetSegments {
				//spawn_seg()
			//}
		//}
		

		log.Printf("\nHeartbeats: %d\n\ntargetSeg: %d\n\nTargetlist: %s \nLotteryNUM: %d\n", ping, targetSegments, targetlist, rticket)
		log.Printf("\nActive list: %s\n", alivelist)
		//time.Sleep(500 * time.Millisecond)

	}

}

func startSegmentServer() {
	// Quit if maxRunTime timeout
	exitReason := make(chan string, 1)
	go func() {
		reason :=  <-exitReason
		log.Printf(reason)
		log.Print("Shutting down")
		os.Exit(0)
	}()

	http.HandleFunc("/", IndexHandler)
	http.HandleFunc("/targetsegments", targetSegmentsHandler)
	http.HandleFunc("/shutdown", shutdownHandler)
	http.HandleFunc("/sync", syncHandler)
	http.HandleFunc("/ticket", lotteryHandler)

	log.Printf("Starting segment server on %s%s\n", hostname, segmentPort)
	log.Printf("Reachable hosts: %s", strings.Join(fetchReachableHosts()," "))


	go heartbeat()

	err := http.ListenAndServe(segmentPort, nil)
	if err != nil {
		log.Panic(err)
	}
}

func IndexHandler(w http.ResponseWriter, r *http.Request) {

	// We don't use the request body. But we should consume it anyway.
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	killRateGuess := 2.0

	fmt.Fprintf(w, "%.3f\n", killRateGuess)
}

func kill_seg() {
	for _, addr := range alivelist[:ping - targetSegments] {
		//targetlist = remove(targetlist, i)
		log.Printf("Host: %s tries to kill: %s", hostname, addr)
		//log.Printf("Targetlist: %s", targetlist)
		//remove(alivelist, addr)
		doWormShutdownPost(addr)
		//time.Sleep(500 * time.Millisecond)
		//kanskje broadcaste hvem som dor
	}

}


func spawn_seg() {
	for _, addr := range targetlist[:targetSegments-ping] {
		//targetlist = remove(targetlist, i)
		log.Printf("Host: %s tries to boot: %s", hostname, addr)
		//log.Printf("Targetlist: %s", targetlist)
		sendSegment(addr)
		//time.Sleep(500 * time.Millisecond)
		doBcastPost(addr)
	}

}
func targetSegmentsHandler(w http.ResponseWriter, r *http.Request) {

	var ts int32
	pc, rateErr := fmt.Fscanf(r.Body, "%d", &ts)
	if pc != 1 || rateErr != nil {
		log.Printf("Error parsing targetSegments (%d items): %s", pc, rateErr)
	}

	// Consume and close rest of body
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	log.Printf("New targetSegments: %d", ts)
	atomic.StoreInt32(&targetSegments, ts)


	if len(alivelist) > 1 {
		for _, addr := range alivelist {
			if addr != hostname || addr+".local" != hostname {
				//Sync targseg
				doBcastPost(addr)
				//time.Sleep(500 * time.Millisecond)

			}
		}
		//start lottery fordi alle har fatt sync
		for _, addr := range alivelist {
			if addr != hostname || addr+".local" != hostname {
				//Start lottery
				doBcastTicket(addr)
				//time.Sleep(500 * time.Millisecond)
				find_winner()
				//time.Sleep(500 * time.Millisecond)
			}
		}
	} else {
		if ping < targetSegments {
			//if hostname == "compute-1-6.local" {
			for _, addr := range targetlist[:targetSegments-ping] {
				//targetlist = remove(targetlist, i)
				//alivelist = append(alivelist, addr)
				sendSegment(addr)
				//time.Sleep(500 * time.Millisecond)
				doBcastPost(addr)
			}

			//for i, addr := range alivelist {
				//if contains(targetlist, addr) == true {
					//targetlist = remove(targetlist, i)
				//}

			//}
		}
	}
}


func shutdownHandler(w http.ResponseWriter, r *http.Request) {

	// Consume and close body
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	// Shut down
	log.Printf("Received shutdown command, committing suicide")
	os.Exit(0)
}

func remove2(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}

func fetchReachableHosts() []string {
	url := fmt.Sprintf("http://localhost%s/reachablehosts", wormgatePort)
	resp, err := http.Get(url)
	if err != nil {
		return []string{}
	}

	var bytes []byte
	bytes, err = ioutil.ReadAll(resp.Body)
	body := string(bytes)
	resp.Body.Close()

	trimmed := strings.TrimSpace(body)
	nodes := strings.Split(trimmed, "\n")


	for i, v := range nodes {
		if v == "compute-1-4" {
			nodes = remove2(nodes, i)
		}
		if v == "compute-2-20" {
			nodes = remove2(nodes, i)
		}
	}
	//return nodes[14:24]
	return nodes
}
