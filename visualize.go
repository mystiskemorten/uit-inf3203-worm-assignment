package main

import (
	"bufio"
	"bytes"
	"fmt"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"./rocks"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const minx, maxx = 1, 3
const miny, maxy = 0, 59
const colwidth = 20
const refreshRate = 200 * time.Millisecond
const pollRate = refreshRate
const pollErrWait = 20 * time.Second

var maxRunTime time.Duration

var wormgatePort string
var segmentPort string

type status struct {
	wormgate  bool
	segment   bool
	err       bool
	rateGuess float32
	rateErr   error
}

var statusMap struct {
	sync.RWMutex
	m map[string]status
}

var killRate int32;
var targetSegments int32;
var partitionScheme int32;

// Use separate clients for wormgates vs segments
//
// There is something about making connections to the same host at different
// ports that confuses the connection caching and reuse. If we just use the
// default Client with the default Transfer, the number of open connections
// balloons during polling until we can't connect anymore. But using separate
// clients for each port (but multiple hosts) works fine.
//
var wormgateClient *http.Client
var segmentClient *http.Client

func createClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{},
	}
}

func main() {
	flag.StringVar(&wormgatePort, "wp", ":8181", "wormgate port (prefix with colon)")
	flag.StringVar(&segmentPort, "sp", ":8182", "segment port (prefix with colon)")
	flag.DurationVar(&maxRunTime, "maxrun", time.Minute*10, "maxtime to run (in case you forget to shut down)")
	flag.Parse()

	nodes := rocks.ListNodes()

	statusMap.m = make(map[string]status)
	for _, node := range nodes {
		statusMap.m[node] = status{}
	}

	targetSegments = 5

	segmentClient = createClient()
	wormgateClient = createClient()

	// Catch interrupt and quit
	interrupt := make(chan os.Signal, 2)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	exitReason := make(chan string, 1)
	go func() {
		//<-interrupt
		time.Sleep(maxRunTime)
		exitReason <- fmt.Sprintf("maxrun timeout: %s", maxRunTime)
	}()

	go func() {
		select {
			case signal := <-interrupt:
				log.Printf("Got signal %s", signal)
			case reason := <-exitReason:
				log.Printf(reason)

		}
		fmt.Print(ansi_clear_to_end)
		fmt.Println()
		log.Print("Shutting down")
		os.Exit(0)
	}()

	// Start poll routines
	for node, _ := range statusMap.m {
		go pollNodeForever(node)
	}

	// Start input routine
	go inputHandler()

	// Start random node killer
	go killNodesForever()

	// Loop display forever
	for {
		printNodeGrid()
		time.Sleep(refreshRate)
	}
}

func pollNodeForever(node string) {
	log.Printf("Starting poll routine for %s", node)
	for {
		s := pollNode(node)
		statusMap.Lock()
		statusMap.m[node] = s
		statusMap.Unlock()
		if s.err {
			time.Sleep(pollErrWait)
		} else {
			time.Sleep(pollRate)
		}
	}
}

func pollNode(host string) status {
	wormgateUrl := fmt.Sprintf("http://%s%s/", host, wormgatePort)
	segmentUrl := fmt.Sprintf("http://%s%s/", host, segmentPort)

	wormgate, _, wgerr := httpGetOk(wormgateClient, wormgateUrl)
	if wgerr != nil {
		return status{false, false, true, 0, nil}
	}
	segment, segBody, segErr := httpGetOk(segmentClient, segmentUrl)

	if segErr != nil {
		return status{false, false, true, 0, nil}
	}

	var rateGuess float32 = 0
	var rateErr error = nil
	if segment {
		var pc int
		pc, rateErr = fmt.Sscanf(segBody, "%f", &rateGuess)
		if pc != 1 || rateErr != nil {
			log.Printf("Error parsing from %s (%d items): %s", host, pc, rateErr)
			log.Printf("Response %s: %s", host, segBody)
		}
	}

	return status{wormgate, segment, false, rateGuess, rateErr}
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

func inputHandler() {
	reader := bufio.NewReader(os.Stdin)

	for {
		input, _ := reader.ReadString('\n')
		log.Printf("Input: %s", input)

		kr := atomic.LoadInt32(&killRate)
		ts := atomic.LoadInt32(&targetSegments)
		ps := atomic.LoadInt32(&partitionScheme)
		shutdown := false

		for _, ch := range input {
			switch ch {
			case 'k':
				kr += 1
			case 'K':
				kr += 10
			case 'j':
				kr -= 1
			case 'J':
				kr -= 10
			case '=': fallthrough
			case '+':
				ts += 1
			case '_': fallthrough
			case '-':
				ts -= 1
			case 's':
				shutdown = true
			case '0':
				ps = 0
			case '1':
				ps = 1
			}
		}
		if kr < 0 {
			kr = 0
		}
		if ts < 1 {
			ts = 1
		}

		fmt.Print(ansi_clear_to_end)

		prevts := atomic.SwapInt32(&targetSegments, ts)
		log.Printf("Target segments: %d -> %d", prevts, ts)

		prevkr := atomic.SwapInt32(&killRate, kr)
		log.Printf("Kill rate: %d -> %d", prevkr, kr)

		prevps := atomic.SwapInt32(&partitionScheme, ps)
		log.Printf("Partition scheme: %d -> %d", prevps, ps)

		if ps!=prevps {
			for _,target := range allWormgateNodes() {
				doPartitionSchemePost(target,ps)
			}
		}

		if ts!=prevts {
			for _,target := range randomSegment() {
				doTargetSegmentsPost(target,ts)
			}
		}

		if shutdown {
			for _,target := range randomSegment() {
				doWormShutdownPost(target)
			}
		}
	}
}

func killNodesForever() {
	for {
		kr := atomic.LoadInt32(&killRate)
		if kr == 0 {
			// do nothing
			time.Sleep(time.Second)
		} else {
			killRandomNode()
			killWait := time.Duration(1000/kr) * time.Millisecond
			time.Sleep(killWait)
		}
	}
}

func randomSegment() []string {
	var segmentNodes []string
	statusMap.RLock()
	for node, status := range statusMap.m {
		if status.segment {
			segmentNodes = append(segmentNodes, node)
		}
	}
	statusMap.RUnlock()
	if len(segmentNodes) > 0 {
		ri := rand.Intn(len(segmentNodes))
		return segmentNodes[ri:ri+1]
	} else {
		return []string{}
	}
}

func allWormgateNodes() []string {
	var nodes []string
	statusMap.RLock()
	for node, status := range statusMap.m {
		if status.wormgate {
			nodes = append(nodes, node)
		}
	}
	statusMap.RUnlock()
	return nodes
}

func killRandomNode() {
	for _,target := range randomSegment() {
		doKillPost(target)
	}
}

func doKillPost(node string) error {
	log.Printf("Killing segment on %s", node)
	url := fmt.Sprintf("http://%s%s/killsegment", node, wormgatePort)
	resp, err := wormgateClient.PostForm(url, nil)
	if err != nil && !strings.Contains(fmt.Sprint(err), "refused") {
		log.Printf("Error killing %s: %s", node, err)
	}
	if err == nil {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
	return err
}

func doPartitionSchemePost(node string, newps int32) error {
	log.Printf("Posting partitionScheme: %d -> %s", newps, node)

	url := fmt.Sprintf("http://%s%s/partitionscheme", node, wormgatePort)
	postBody := strings.NewReader(fmt.Sprint(newps))

	resp, err := wormgateClient.Post(url, "text/plain", postBody)
	if err != nil && !strings.Contains(fmt.Sprint(err), "refused") {
		log.Printf("Error posting partitionScheme %s: %s", node, err)
	}
	if err == nil {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
	return err
}

func doTargetSegmentsPost(node string, newts int32) error {
	log.Printf("Posting targetSegments: %d -> %s", newts, node)

	url := fmt.Sprintf("http://%s%s/targetsegments", node, segmentPort)
	postBody := strings.NewReader(fmt.Sprint(newts))

	resp, err := segmentClient.Post(url, "text/plain", postBody)
	if err != nil && !strings.Contains(fmt.Sprint(err), "refused") {
		log.Printf("Error posting shutdown to %s: %s", node, err)
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

const ansi_bold = "\033[1m"
const ansi_reset = "\033[0m"
const ansi_reverse = "\033[30;47m"
const ansi_red_bg = "\033[30;41m"
const ansi_clear_to_end = "\033[0J"

func ansi_down_lines(n int) string {
	return fmt.Sprintf("\033[%dE", n)
}
func ansi_up_lines(n int) string {
	return fmt.Sprintf("\033[%dF", n)
}

func printNodeGrid() {
	statusMap.RLock()

	gridBuf := bytes.NewBuffer(nil)
	rateGuesses := make([]float32, 0, len(statusMap.m))

	fmt.Fprint(gridBuf, ansi_clear_to_end)
	fmt.Fprintln(gridBuf)
	fmt.Fprintln(gridBuf)
	fmt.Fprint(gridBuf, "Legend: ")
	fmt.Fprint(gridBuf, "node,  ")
	fmt.Fprint(gridBuf, ansi_bold, "wormgate", ansi_reset, ",  ")
	fmt.Fprint(gridBuf, ansi_reverse, "segment", ansi_reset, ",  ")
	fmt.Fprint(gridBuf, ansi_red_bg, "error", ansi_reset)
	fmt.Fprintln(gridBuf)
	fmt.Fprint(gridBuf, "Keys  :")
	fmt.Fprint(gridBuf, "  kK/jJ kill rate,")
	fmt.Fprint(gridBuf, "  +/- segments,")
	fmt.Fprint(gridBuf, "  0-9 partition,")
	fmt.Fprint(gridBuf, "  s worm shutdown,")
	fmt.Fprint(gridBuf, "  Ctrl-C quit")

	for x := minx; x <= maxx; x++ {
		for y := miny; y <= maxy; y++ {
			if y%colwidth == 0 {
				fmt.Fprintf(gridBuf, "\n%d: %02d+", x, y/colwidth*colwidth)
			}
			if y%10 == 0 {
				fmt.Fprintf(gridBuf, "|")
			}
			node := fmt.Sprintf("compute-%d-%d", x, y)
			status, nodeup := statusMap.m[node]

			var char string
			if nodeup {
				char = fmt.Sprint(y % 10)
			} else {
				char = " "
			}

			if status.err {
				fmt.Fprint(gridBuf, ansi_red_bg)
			} else {
				if status.wormgate {
					fmt.Fprint(gridBuf, ansi_bold)
				}
				if status.segment {
					fmt.Fprint(gridBuf, ansi_reverse)
				}
				if status.segment && status.rateErr == nil {
					rateGuesses = append(rateGuesses,
						status.rateGuess)
				}
			}
			fmt.Fprint(gridBuf, char)
			fmt.Fprint(gridBuf, ansi_reset)
		}
	}
	statusMap.RUnlock()
	fmt.Fprintln(gridBuf)

	ts := atomic.LoadInt32(&targetSegments)
	fmt.Fprintf(gridBuf, "Target number of segments: %d\n", ts)

	kr := atomic.LoadInt32(&killRate)
	fmt.Fprintf(gridBuf, "Kill rate: %d/sec\n", kr)
	fmt.Fprintf(gridBuf, "Avg guess: %.1f/sec (%d segments reporting)\n",
		mean(rateGuesses), len(rateGuesses))

	fmt.Fprintln(gridBuf, time.Now().Format(time.StampMilli))
	var gridLines = bytes.Count(gridBuf.Bytes(), []byte("\n"))
	fmt.Fprint(gridBuf, ansi_up_lines(gridLines))
	io.Copy(os.Stdout, gridBuf)
}

func mean(floats []float32) float32 {
	var sum float32 = 0
	for _, f := range floats {
		sum += f
	}
	return sum / float32(len(floats))
}
