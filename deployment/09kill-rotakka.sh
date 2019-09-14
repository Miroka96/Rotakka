#!/usr/bin/env bash
#!/bin/bash

for ip in $(cat ips); do
	# spawns a child process
	sshpass -p cluster ssh student@$ip "pkill -f Rotakka; pkill -f java" &
done

# waits for all children spawned above
wait