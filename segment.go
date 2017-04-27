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


func main() {

	hostname, _ = os.Hostname()
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
		log.Printf("Error synch %s: %s", node, err)
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
		log.Printf("Error parsing targetSegments (%d items): %s", pc, rateErr)
	}

	atomic.StoreInt32(&targetSegments, ts)
	// Consume and close body
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	// Shut down
	log.Printf("Received sync command")
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func remove(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}

func heartbeat() {
	segmentClient = createClient()
	list := fetchReachableHosts()
	targetlist = fetchReachableHosts()

	for {
		ping = 0
		for i, addr := range list {
			if addr == hostname || addr+".local" == hostname {
				ping++
				if contains(alivelist, addr) == false {
					alivelist = append(alivelist, addr)
				}
				if contains(targetlist, addr) == true {
					targetlist = remove(targetlist, i)
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
						alivelist = append(alivelist, addr)
					}
					if contains(targetlist, addr) == true {
						targetlist = remove(targetlist, i)
					}
				}

			//if segErr != nil {
				//ping++
			//}
			}
		}

		log.Printf("\nHeartbeats: %d\n\ntargetSeg: %d\n", ping, targetSegments)
		log.Printf("\nActive list: %s\n", alivelist)
		time.Sleep(500 * time.Millisecond)

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
			if addr+".local" != hostname {
				doBcastPost(addr)

			}
		}

	} else {

		if ping < targetSegments {
			if hostname == "compute-1-6.local" {
				for _, addr := range targetlist[:targetSegments-ping] {
					//targetlist = remove(targetlist, i)
					sendSegment(addr)
					time.Sleep(500 * time.Millisecond)
					doBcastPost(addr)
				}

			}

		} else {
			//poster killtime
			//one tries to kill others lay off
			//if he dont report kill success, others need to heartbeat to check if he died
			//and then go back to either increment or decrease state

			//First one to say I will kill myself
			for _, addr := range alivelist {
				doBcastPost(addr)
			}
			//The others will then need to heartbeat to see who dissappread
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
			nodes = remove(nodes, i)
		}
		if v == "compute-2-20" {
			nodes = remove(nodes, i)
		}
	}
	return nodes
}
