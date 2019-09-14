# Rotakka

Rotakka is a distributed cluster application designed for scalable Twitter crawling. 
Its main advantage is that it avoids IP-based blocking by exploiting publicly available web proxies. 
In contrast to API-based approaches, Rotakka uses browser emulation enabled by Selenium to visit and download Twitter user profiles. 
It is built on the Akka framework and consists of 
* a proxy-collecting module, 
* a proxy-checking module, 
* a Twitter-crawling module, 
* and a graph-storing module. 

## Requirements

* Java 8
* Maven
* a working Selenium driver, in our case:
  * an installed Google Chrome or Chromium browser
  * a downloaded chromedriver binary of the same version as the Chrome browser
* following environment variables must be set:
  * CHROME_DRIVER_PATH
    * our value: /usr/bin/chromedriver
  * CHROME_BINARY_PATH
    * our value: /usr/bin/google-chrome-stable 
  * CHROME_HEADLESS_MODE
    * on servers: true
    * for visual development: false
    
For further instructions, have a look at the scripts in the "deployment" directory.

## Usage

### Building a Fat-JAR

```
mvn package
```

The Jar will be created in the "target" directory.

### Running the Fat-JAR
```
java [-Drotakka.config.parameter="whatever"] -jar rotakka-1.0.jar
```

There can be multiple config parameters added, each prepended with "-D".

At the end of the command above, either "master" or "slave" must follow, otherwise the help is printed.

### Developing with IntelliJ

Just import the project as Maven project.

#### Master configuration
* ProgramArguments="master"
* EnvironmentVariables=... (set them as mentioned above)

#### Slave configuration
* ProgramArguments="slave -mh 127.0.0.1"
* EnvironmentVariables=... (set them as mentioned above)

### Useful Config Parameters
(all others are defined ...)

