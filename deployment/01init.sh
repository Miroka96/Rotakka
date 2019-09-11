#!/bin/bash

for ip in $(cat ips); do
	ssh student@$ip hostname
done

