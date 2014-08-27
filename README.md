# Wormgate
An example wormgate and segment code for the first assignment of the INF-3200
Distributed Systems Fundamentals course fall 2014. The Wormgate (wormgate.go) is
a simple program that starts a HTTP server and waits for a POST request to
/segment. It stores whatever it receives to /tmp/wormgate/[random hex string]/tmp.tar.gz,
attempts to extract this, runs hello-world-graphic

# How To

Start the wormgate

``` 
go run wormgate.go
``` 

Send the segment code

```
go run ctrlman.go
```

The source code for the segment code can be found in hello-world-graphic.go

NB that you might have to rebuild the segment binary (do this if you get a weird exec error from the wormgate):

```
go build hello-world-graphic.go
```
