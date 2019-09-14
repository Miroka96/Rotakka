#!/usr/bin/env bash
#!/bin/bash

for ip in $(cat ips); do
	# spawns a child process
	sshpass -p cluster ssh student@$ip "echo NEW SLAVE - TOTAL TWEETS; cd Rotakka; cd logs; cat app.log | grep 'Total Scraped Tweets:'"
	sshpass -p cluster ssh student@$ip "echo NEW SLAVE - TOTAL SCRAPED USERS; cd Rotakka; cd logs; cat app.log | grep 'Total Scraped Users:'"
done

# waits for all children spawned above
wait