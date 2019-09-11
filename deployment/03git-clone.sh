#!/bin/bash

for ip in $(cat ips); do
	# spawns a child process
	sshpass -p cluster ssh student@$ip git clone https://github.com/Miroka96/Rotakka.git &
done

# waits for all children spawned above
wait
