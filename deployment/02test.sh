#!/bin/bash

for ip in $(cat ips); do
	# spawns a child process
	sshpass -p cluster ssh student@$ip hostname &
done

# waits for all children spawned above
wait
