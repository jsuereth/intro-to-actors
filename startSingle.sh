#!/bin/bash


sbt -Dsbt.log.noformat=true "run-main Main 2551 true" >cluster1.log 2>&1 &
pid1=$!
gnome-terminal --title=Cluster1Log  -e "less -++F cluster1.log" &


echo "Server running.  Press enter to shutdown...."
read

kill $pid1
