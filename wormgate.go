package main

import (
	"./rocks"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var maxRunTime time.Duration

var wormgatePort string

var path string

var hostname string
var allHosts []string
var partitionScheme int32

var runningSegment struct {
	sync.RWMutex
	p *os.Process
}

func main() {

	flag.StringVar(&wormgatePort, "wp", ":8181", "wormgate port (prefix with colon)")
	flag.DurationVar(&maxRunTime, "maxrun", time.Minute*10, "max time to run (in case you forget shut down)")
	flag.Parse()

	allHosts = rocks.ListNodes()

	hostname, _ = os.Hostname()
	log.SetPrefix(hostname + " wormgate: ")

	curuser, err := user.Current()
	if err != nil {
		log.Panic(err)
	}
	log.Printf("Current user: %s\n", curuser.Username)

	path = *flag.String("path", "/tmp/wormgate-"+curuser.Username,
		"where to store segment code")

	log.Printf("Changing working directory to " + path)
	os.Chdir(path)

	rand.Seed(time.Now().Unix())

	flag.Parse()

	// Quit if maxRunTime timout
	exitReason := make(chan string, 1)
	go func() {
		time.Sleep(maxRunTime)
		exitReason <- fmt.Sprintf("maxrun timeout: %s", maxRunTime)
	}()
	go func() {
		reason := <-exitReason
		log.Printf(reason)
		log.Print("Shutting down")
		os.Exit(0)
	}()

	http.HandleFunc("/", IndexHandler)
	http.HandleFunc("/wormgate", WormGateHandler)
	http.HandleFunc("/killsegment", killSegmentHandler)
	http.HandleFunc("/partitionscheme", partitionSchemeHandler)
	http.HandleFunc("/reachablehosts", reachableHostsHandler)

	log.Printf("Started wormgate on %s%s\n", hostname, wormgatePort)

	err = http.ListenAndServe(wormgatePort, nil)

	if err != nil {
		log.Panic(err)
	}
}

func WormGateHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	runningSegment.Lock()
	defer runningSegment.Unlock()

	if runningSegment.p != nil {
		http.Error(w, "Segment already running", 409)

		// Consume body
		io.Copy(ioutil.Discard, r.Body)
		r.Body.Close()

		return
	}

	var segmentPort = r.URL.Query().Get("sp")

	log.Println("Received segment from", r.RemoteAddr)

	// we'll extract and execute our segment in a new folder
	randomstring := fmt.Sprintf("%x", rand.Int63())
	extractionpath := path + "/" + randomstring
	filename := "tmp.tar.gz"
	fn := extractionpath + "/" + filename

	// Create directory to store segment code.
	err := os.MkdirAll(extractionpath, 0755)
	if err != nil {
		log.Panic("Could not create directory to store segment ", err)
	}
	os.Chdir(extractionpath)
	defer os.Chdir(path) // change back to base directory later

	// Create file and store incoming segment
	file, err := os.Create(fn)
	if err != nil {
		log.Panic("Could not create file to store segment", err)
	}
	defer os.Remove(fn) // let's remove the tarball later

	// Read from http POST
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		// Could not read body from POST.
		// Probably the segment was killed while trying to send.
		// That's the worm's problem, not ours. So just abort.
		log.Print("Error reading payload.", err)
		return
	}

	// Write tarball to file
	_, err = file.Write(body)
	if err != nil {
		log.Print("Error writing payload file.", err)
		return
	}

	err = file.Close()
	if err != nil {
		log.Print("Error closing payload file.", err)
		return
	}

	// extract segment
	cmdline := []string{"tar", "-xzf", fn}
	log.Printf("Extracting segment: %q", cmdline)
	tarCmd := exec.Command(cmdline[0], cmdline[1:]...)
	err = tarCmd.Run()
	if err != nil {
		log.Print("Error extracting segment.", err)
		return
	}

	// Start command, do not wait for it to complete
	binary := extractionpath + "/" + "segment"
	cmdline = []string{"stdbuf", "-oL", "-eL",
			//binary, "run", "-wp", wormgatePort, "-sp", segmentPort}
			binary, "run", "-wp", wormgatePort, "-sp", segmentPort, "-maxrun", maxRunTime.String()}

	log.Printf("Running segment: %q", cmdline)
	cmd := exec.Command(cmdline[0], cmdline[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	//cmd.Dir = path
	err = cmd.Start()
	if err != nil {
		log.Print("Error starting segment ", err)
		return
	}
	runningSegment.p = cmd.Process

	go func() {
		// Wait for process to end and reset the process pointer
		runningSegment.RLock()
		p := runningSegment.p
		runningSegment.RUnlock()

		p.Wait()

		runningSegment.Lock()
		runningSegment.p = nil
		runningSegment.Unlock()
	}()
}

func killSegmentHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// We don't use the body, but read it anyway
	io.Copy(ioutil.Discard, r.Body)

	runningSegment.Lock()
	if runningSegment.p != nil {
		pid := runningSegment.p.Pid
		log.Printf("Killing segment process %d", pid)
		err := runningSegment.p.Kill()
		if err != nil {
			log.Panicf("Could not kill segment process %d: %s",
				pid, err)
		}
		runningSegment.p = nil
		fmt.Fprintf(w, "Killed segment process %d\n", pid)
	} else {
		msg := "No segment process to kill\n"
		log.Printf(msg)
		fmt.Fprintf(w, msg)
	}
	runningSegment.Unlock()
}

func IndexHandler(w http.ResponseWriter, r *http.Request) {

	// We don't use the body, but read it anyway
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	body := "Wormgate running on " + hostname
	fmt.Fprintf(w, "<h1>%s</h1></br><p>Post segments to to /segment</p>", body)
}

func reachableHostsHandler(w http.ResponseWriter, r *http.Request) {
	// We don't use the body, but read it anyway
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	for _,host := range reachableHosts() {
		fmt.Fprintln(w, host)
	}
}

func reachableHosts() []string {
	var reachable []string

	ps := atomic.LoadInt32(&partitionScheme)
	if ps==0 {
		for _,host := range allHosts {
			reachable = append(reachable, host)
		}
	}
	if ps==1 {
		for _,host := range allHosts {
			n := len("compute-x")
			if host[0:n] == hostname[0:n] {
				reachable = append(reachable, host)
			}
		}
	}
	return reachable
}

func partitionSchemeHandler(w http.ResponseWriter, r *http.Request) {

	var ps int32
	pc, rateErr := fmt.Fscanf(r.Body, "%d", &ps)
	if pc != 1 || rateErr != nil {
		log.Printf("Error parsing partitionScheme (%d items): %s", pc, rateErr)
	}

	// Consume and close rest of body
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	log.Printf("New partitionScheme: %d", ps)
	atomic.StoreInt32(&partitionScheme, ps)
	log.Printf("Reachable hosts: %s", strings.Join(reachableHosts()," "))
}
