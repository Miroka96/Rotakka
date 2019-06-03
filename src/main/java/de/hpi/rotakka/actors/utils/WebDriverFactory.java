package de.hpi.rotakka.actors.utils;

import akka.event.LoggingAdapter;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;


public class WebDriverFactory {

    final private LoggingAdapter log;

    static private WebDriver webDriver;

    static public WebDriver getWebDriver(LoggingAdapter log) {
        if (webDriver == null) {
            WebDriverFactory factory = new WebDriverFactory(log);
            webDriver = factory.createWebDriver();
        }
        return webDriver;
    }

    private WebDriverFactory(LoggingAdapter log) {
        this.log = log;
    }

    private WebDriver createWebDriver() {
        WebDriver webDriver;

        webDriver = createSeleniumWebDriver();
        if (webDriver != null) {
            return webDriver;
        }

        log.error("no web driver available for crawler");
        return null;
    }

    private WebDriver createSeleniumWebDriver() {
        // HINT: Don't forget to .close() the WebDriver
        // HINT: Have only one WebDriver for all threads
        // HINT: Make the WebDriver interaction your custom get()-method thread safe
        String envChromeDriverPath = System.getenv("CHROME_DRIVER_PATH");
        String envChromeBinaryPath = System.getenv("CHROME_BINARY_PATH");

        if (envChromeDriverPath == null) {
            log.error("Chrome Driver Path not set");
            return null;
        }
        if (envChromeBinaryPath == null) {
            log.error("Chrome Binary Path not set");
            return null;
        }

        System.setProperty("webdriver.chrome.driver", envChromeDriverPath);
        String proxyAddress = System.getenv("PROXY_ADDRESS");


        ChromeOptions options = new ChromeOptions();

        options.addArguments("--no-sandbox");
        options.addArguments("headless");
        options.addArguments("--disable-gpu");
        options.addArguments("disable-infobars");
        options.addArguments("--disable-extensions");
        options.addArguments("window-size=1200x600");

        options.setBinary(envChromeBinaryPath);

        if (proxyAddress != null && !proxyAddress.isEmpty()) {
            options.addArguments("--proxy-server=http://" + proxyAddress);
        }

        return new ChromeDriver(options);
    }
}
