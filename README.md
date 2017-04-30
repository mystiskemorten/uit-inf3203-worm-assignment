Worm Assignment Code
==================================================

This is the starter code for a mandatory assignment for the Advanced Distributed
Systems course (INF-3203) at the University of Tromsø.

The code is primarily written in Go and is designed to run on our Rocks cluster
at the university.

This document is primarily for technical details about the code. See the
[assignment PDF](assignment.pdf) for more general information about the
assignment.


Source files
--------------------------------------------------

Go source files:

- wormgate.go -- the worm gate server
- segment.go -- code for the worm itself
- visualize.go -- a simple command and report center for the worm
- rocks/rocks.go -- library for working with the rocks cluster

Support scripts:

- build.sh -- simple script to build the included Go files
- ssh-all.sh -- utility script to run a given process on all compute nodes
- clean-worm.sh -- script to stop all processes and clean up files on the
  compute nodes


Concept
--------------------------------------------------

The worm gate (wormgate.go) is a server that you will run on several compute
nodes in the cluster. The worm segment (segment.go) will zip itself up (tar.gz,
actually) and POST the tarball to a worm gate. The worm gate will open the
tarball file it receives and then run the worm segment code inside. When the
worm segment runs on a compute node, it will also run as a server so that it can
communicate with other segments and the command and report center
(visualize.go).

The command and report center / visualizer will poll all nodes in the cluster
for the presence of the worm gate and worm segment. It will print out a grid
with color codes to represent the status of each node. It's primitive, but it
should let us watch the worm move around the network.



What's provided vs what's expected
--------------------------------------------------

We have provided the worm gate, the command and report center / visualizer, and
an empty shell of a worm segment. Your task is to fill in the worm segment code
to build an actual worm.

- The worm should propagate to other worm gates in the network. It should try to
  stay alive and to keep itself at a target number of segments, no more, no
  less.

- To simulate attempts to stop the worm, worm gates will kill their host nodes
  at a given rate (x kills per second). Your worm should try to estimate that
  kill rate and report it when asked.

- We want to see how the worm reacts to different network topographies and
  network partitions, but the cluster's network is frustratingly reliable. So to
  simulate different network connections, each worm gate will have a list of
  nodes that it considers "reachable". Your worm should consult this list on its
  host worm gate before contacting another worm gate or segment. This list will
  change during operation, so the worm segment must check it frequently.

- Your worm's job will be to relentlessly stay alive, but it must also shut down
  reliably when asked.

See the "Worm gate and worm segment API" section for details about the API for
these commands.


Quick Start
--------------------------------------------------

NOTE: Be sure to change these ports so that you don't conflict with other
students.

Set up Go environment variables. Add the following to your `~/.bashrc` and log
out/in again:

    # golang
    export GOROOT=/share/apps/go/
    export PATH=$PATH:$GOROOT/bin/

Build the Go source:

    ./build.sh

Start the command and report center / visualizer (press Ctrl+C to exit):

    ./visualize -wp :9037 -sp :9040

Start a worm gate on one compute node (you'll want to do this in another
terminal window so you can leave the visualizer running):

    ssh compute-1-1 "$PWD/wormgate" -wp :8181 -sp :8182

Spread the worm to the compute node:

    ./segment spread -wp :8181 -sp :8182 -host compute-1-1

Kill all of your processes on all compute nodes and clean up temporary files:

    ./clean-worm.sh


Visualizer controls
--------------------------------------------------

The visualizer program is also the command center for the worm. It issues
commands to the worm gates and segments. To enter commands, type a series of
command characters and then press enter.

Command characters:

- To the worm segments:

    - `+`: increase the target number of segments
    - `-`: decrease the target number of segments

- To the worm gates:

    - `0`-`9`: switch simulated partition schemes

- For the visualizer itself:

    - `k`: increase kill rate by 1 kill/sec
    - `K`: increase kill rate by 10 kill/sec
    - `j`: decrease kill rate by 1 kill/sec
    - `J`: decrease kill rate by 10 kill/sec


Worm gate and worm segment API
--------------------------------------------------

The components communicate via HTTP. Your worm may use a different protocol for
communication between segments, but it must continue to support the HTTP API
specified here.

### Worm gate

You should not have to make any changes to the worm gate, unless you need to
pass additional command line parameters to the segment when launching.

Command line:

- Start the worm get server on the given port (remember to change ports!). To be
  run on several compute nodes via SSH. The `-wp` parameter specifies the worm
  gate port.

        # Local
        ./wormgate -wp :8181

        # Via SSH to a single compute node
        ssh -f compute-1-1 "$PWD/wormgate" -wp :8181

        # On all compute nodes
        ./ssh-all.sh "$PWD/wormgate" -wp :8181

HTTP API:

- `GET /` -- Welcome page. The visualizer will poll this resource to check that
  the worm gate is alive. The content doesn't matter.

- `POST /wormgate?sp=:8182` (tarball) -- Worm segment entrance. The worm segment
  will post itself to this resource as a tarball, and the worm gate will receive
  the file here, save it to a temporary directory, extract it, and run the worm
  segment inside. The query parameter `sp` specifies the segment port number to
  pass to the segment when it starts (via the `-sp` command line parameter).

- `POST /killsegment` (no content) -- Worm segment kill command. The visualizer
  will post to this resource to ask the worm gate to kill the segment that it is
  hosting. This is how the kill rate works: X times per second, the visualizer
  will pick a random node that has a worm segment and send this command to its
  worm gate.

- `GET /reachablehosts` -- List of "reachable" hosts. We will use this to
  simulate network partitions. Your worm segment should consult this resource on
  its own host before each request to see if the destination host is reachable.
  If the host is not in the list, your segment is not allowed to communicate
  with it.

    - The format is a simple list of host names, one per line.
    - For performance, your segments may cache the result of the query, but the
      time-to-live should be short. No less frequent than once per second.

- `POST /partitionscheme` (integer) -- Command to switch simulated partition
  schemes. This will affect the output of the reachable hosts query. The
  visualizer will post this command to all running worm gates when the user
  switches schemes. There are only two partition schemes supported at this
  point. We may add more for the demo. You are encouraged to add more of your
  own.

    - 0: no partition
    - 1: by first digit of compute name: compute-1-x / compute-2-x / compute-3-x

### Worm segment

Again, your task is to get the worm segments coordinating and acting as a
unified worm. You may add whatever communication resources or protocols you like
to make that happen, but your segments much continue to support this API so that
it can work with the worm gate and visualizer.

Command line:

- Spread mode -- This command will have the segment tar itself up and POST
  itself to the given worm gate (`-host`) at the given port (`-wp`). The worm
  gate will then run it (run mode) with the given segment port (`-sp`). You can
  run this command locally.

        # Run locally to spread to a single host
        ./segment spread -wp :8181 -sp :8182 -host compute-1-1

- Run mode -- You normally won't have to run this directly. This is the command
  the worm gate will use to start the segment as a server on the given port
  (`-sp`). The segment can then contact the local worm gate that launched it at
  the given worm gate port (`-wp`). Don't forget to use the reachable hosts
  resource to get the list of hosts the worm is allowed to contact.

        # Run locally by the worm gate when it receives a segment package
        ./segment run -wp :8181 -sp :8182

HTTP API:

- `GET /` -- Get kill rate estimate. The visualizer will poll this resource to
  check if the segment is running and to collect its kill rate estimate. The
  format should be plain text, with just the kill rate as a single
  floating-point number.

- `POST /targetsegments` (integer) -- Set target number of segments. When the
  user changes the target number of segments, the visualizer will post to this
  resource. The content will just be a single integer in plan text, the target
  number of segments. Only one segment will be notified of the change, and that
  segment must propagate the information to the rest of the worm, and coordinate
  to grow or shrink the worm to the target number of segments.

- `POST /shutdown` (no content) -- Worm shutdown (suicide) command. When the
  user requests that the worm shut down, the visualizer will post to this
  resource on a random segment. Upon receiving this command to any segment, the
  entire worm should coordinate to shut down.


Other handy commands
--------------------------------------------------

`ssh-all.sh` is a little support script that will loop and run a command on
every node in the cluster:

    # Start wormgate on all compute nodes
    ./ssh-all.sh "$PWD/wormgate" -wp :8181

    # Kill segment on all compute nodes
    ./ssh-all.sh killall segment

    # Kill worm gate on all compute nodes
    ./ssh-all.sh killall wormgate

    # Kill (SIGTERM) all of your processes on all compute nodes
    ./ssh-all.sh killall -u $(whoami)

    # Really kill (SIGKILL) all of your processes on all compute nodes
    ./ssh-all.sh killall -9 -u $(whoami)

    # Clean up temporary files (the untarred segments) on all compute nodes
    ./ssh-all.sh rm -r /tmp/wormgate-$(whoami)

`clean-worm.sh` is a short script that does the kill all and cleanup commands
back to back.

    # Stop all your processes and remove temporary files on all compute nodes
    ./clean-worm.sh
