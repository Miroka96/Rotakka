package de.hpi.rotakka.actors.utils;

import akka.actor.ActorContext;
import akka.event.LoggingAdapter;
import de.hpi.rotakka.actors.proxy.CheckedProxy;
import org.jetbrains.annotations.Nullable;
import org.openqa.selenium.SessionNotCreatedException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;

public class WebDriverFactory {

    @Nullable
    public static WebDriver createWebDriver(LoggingAdapter logger, ActorContext context) {
        WebDriver webDriver;
        webDriver = createSeleniumWebDriver(logger, null);

        if (webDriver != null) {
            return webDriver;
        } else {
            logger.error("Selenium could not be started, shutting down system");
            context.system().terminate();
            return null;
        }
    }

    @Nullable
    public static WebDriver createWebDriver(LoggingAdapter logger, ActorContext context, CheckedProxy proxy) {
        WebDriver webDriver;
        webDriver = createSeleniumWebDriver(logger, proxy);

        if (webDriver != null) {
            return webDriver;
        } else {
            logger.error("Selenium could not be started, shutting down system");
            context.system().terminate();
            return null;
        }
    }

    @Nullable
    private static WebDriver createSeleniumWebDriver(LoggingAdapter logger, CheckedProxy proxy) {
        // HINT: Don't forget to .close() the WebDriver
        // HINT: Have only one WebDriver for all threads
        // HINT: Make the WebDriver interaction your custom get()-method thread safe

        String envChromeDriverPath = System.getenv("CHROME_DRIVER_PATH");
        String envChromeBinaryPath = System.getenv("CHROME_BINARY_PATH");

        if (envChromeDriverPath == null) {
            logger.error("Chrome Driver Path not set, terminating soon");
            return null;
        }
        if (envChromeBinaryPath == null) {
            logger.error("Chrome Binary Path not set, terminating soon");
            return null;
        }
        boolean envHeadlessMode;
        if (System.getenv("CHROME_HEADLESS_MODE") == null) {
            logger.warning("Chrome Headless Mode not set");
            envHeadlessMode = true;
        } else {
            envHeadlessMode = Boolean.parseBoolean(System.getenv("CHROME_HEADLESS_MODE"));
        }

        System.setProperty("webdriver.chrome.driver", envChromeDriverPath);
        ChromeOptions options = new ChromeOptions();
        options.setBinary(envChromeBinaryPath);

        if (proxy != null) {
            String proxyAddress = proxy.getIp() + ":" + proxy.getPort();
            options.addArguments("--proxy-server=http://" + proxyAddress);
        }

        if (envHeadlessMode) {
            options.addArguments("--no-sandbox");
            options.addArguments("headless");
            options.addArguments("--disable-gpu");
            options.addArguments("disable-infobars");
            options.addArguments("--disable-extensions");
            options.addArguments("window-size=1200x600");
        }

        try {
            return new ChromeDriver(options);
        } catch (SessionNotCreatedException e) {
            logger.error(e, "Selenium Session could not be created, terminating soon");
            return null;
        }
    }
}
