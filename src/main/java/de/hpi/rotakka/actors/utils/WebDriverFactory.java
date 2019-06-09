package de.hpi.rotakka.actors.utils;

import akka.actor.ActorContext;
import akka.event.LoggingAdapter;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;

public class WebDriverFactory {

    public static WebDriver createWebDriver(LoggingAdapter logger, ActorContext context) {
        WebDriver webDriver;
        webDriver = createSeleniumWebDriver(logger);

        if (webDriver != null) {
            return webDriver;
        }
        else {
            logger.error("ERROR: Selenium could not be started, shutting down system");
            context.system().terminate();
            return null;
        }
    }

    private static WebDriver createSeleniumWebDriver(LoggingAdapter logger) {
        // HINT: Don't forget to .close() the WebDriver
        // HINT: Have only one WebDriver for all threads
        // HINT: Make the WebDriver interaction your custom get()-method thread safe
        String envChromeDriverPath = System.getenv("CHROME_DRIVER_PATH");
        String envChromeBinaryPath = System.getenv("CHROME_BINARY_PATH");
        boolean envHealessMode = Boolean.parseBoolean(System.getenv("CHROME_HEADLESS_MODE"));

        if (envChromeDriverPath == null) {
            logger.error("Chrome Driver Path not set, terminating");
            return null;
        }
        if (envChromeBinaryPath == null) {
            logger.error("Chrome Binary Path not set, terminating");
            return null;
        }
        if (System.getenv("CHROME_HEADLESS_MODE") == null) {
            logger.error("Chrome Headless Mode is set; terminating");
            return null;
        }


        System.setProperty("webdriver.chrome.driver", envChromeDriverPath);
        String proxyAddress = System.getenv("PROXY_ADDRESS");

        ChromeOptions options = new ChromeOptions();

        if(envHealessMode) {
            options.addArguments("--no-sandbox");
            options.addArguments("headless");
            options.addArguments("--disable-gpu");
            options.addArguments("disable-infobars");
            options.addArguments("--disable-extensions");
            options.addArguments("window-size=1200x600");
        }

        options.setBinary(envChromeBinaryPath);

        if (proxyAddress != null && !proxyAddress.isEmpty()) {
            options.addArguments("--proxy-server=http://" + proxyAddress);
        }

        return new ChromeDriver(options);
    }
}
