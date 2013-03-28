#!/bin/bash


sbt "run-main Main 2551 true" >cluster1.log 2>&1 &
pid1=$!
gnome-terminal --title=Cluster1Log  -e "less -++F cluster1.log" &
sleep 20
sbt "run-main Main 2552 false" >cluster2.log 2>&1  &
pid2=$!


gnome-terminal --title=Cluster2Log -e "less -++F cluster2.log" &

echo "Server running.  Press enter to shutdown...."
read

kill $pid1 $pid2
