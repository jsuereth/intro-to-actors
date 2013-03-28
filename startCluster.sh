#!/bin/bash


sbt "run-main Main 2551 true" >cluster1.log 2>&1 &
pid1=$!
sbt "run-main Main 2552 true" >cluster2.log 2>&1  &
pid2=$!


gnome-terminal --title=Cluster1Log  -e "less -++F cluster1.log" &
gnome-terminal --title=Cluster2Log -e "less -++F cluster2.log" &

echo "Server running.  Press enter to shutdown...."
read

kill -9 $pid1 $pid2
