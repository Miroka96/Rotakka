#!/bin/bash

master=$(head -n 1 ips)

environment="CHROME_DRIVER_PATH=/usr/bin/chromedriver CHROME_BINARY_PATH=/usr/bin/google-chrome-stable CHROME_HEADLESS_MODE=true"

command="shopt -s huponexit; cd Rotakka; rm -r logs; rm -r shards; $environment java -Drotakka.twittercrawling.slaveCount=16 -jar target/rotakka-1.0.jar"

sshpass -p cluster ssh -t -t student@$master "$command master" &

for ip in $(tail -n 1 ips); do
	# spawns a child process
	sshpass -p cluster ssh -t -t student@$ip "$command slave --masterhost $master" &
done

# waits for an Enter
read

pkill -P $$
