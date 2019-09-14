#!/bin/bash

master=$(head -n 1 ips)

environment="CHROME_DRIVER_PATH=/usr/bin/chromedriver CHROME_BINARY_PATH=/usr/bin/google-chrome-stable CHROME_HEADLESS_MODE=true"
slaveCount="-Drotakka.twittercrawling.slaveCount=16"
entryPoints="-Drotakka.twittercrawling.entryPointUsers='BBCWorld,guardian,sternde,SPIEGELONLINE,nytimes,washingtonpost,WSJ,USATODAY'"
startDate="-Drotakka.twittercrawling.startDate='01-01-2018'"
endDate="-Drotakka.twittercrawling.endDate='01-01-2019'"
extractUsers="-Drotakka.twittercrawling.extractUsers=false"


command="shopt -s huponexit; cd Rotakka; rm -r logs; rm -r shards; $environment java $slaveCount $entryPoints $startDate $endDate $extractUsers -jar target/rotakka-1.0.jar"

sshpass -p cluster ssh -t -t student@$master "$command master" &

for ip in $(tail -n 11 ips); do
	# spawns a child process
	echo "Starting Child"
	sshpass -p cluster ssh -t -t student@$ip "$command slave --masterhost $master" &
done

# waits for an Enter
read

pkill -P $$
