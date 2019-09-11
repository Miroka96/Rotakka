#!/bin/bash

for ip in $(cat ips); do
	# spawns a child process
	sshpass -p cluster ssh student@$ip "cd Rotakka ; mvn package" &
done

# waits for all children spawned above
wait
