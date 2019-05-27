package de.hpi.rotakka.actors.utils;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;

import java.util.logging.Logger;

public class Utility {

    public static WebDriver createSeleniumWebDriver(Boolean headlessMode, ChromeOptions options){
        // HINT: Don't forget to .close() the WebDriver
        // HINT: Have only one WebDriver for all threads
        // HINT: Make the WebDriver interaction your custom get()-method thread safe
        String envChromeDriverPath = System.getenv("CHROME_DRIVER_PATH");
        String envChromeBinaryPath = System.getenv("CHROME_BINARY_PATH");

        if (envChromeDriverPath == null) Logger.getGlobal().warning("ERROR: Chrome Driver Path not set");
        if (envChromeBinaryPath == null) Logger.getGlobal().warning("ERROR: Chrome Binary Path not set");

        System.setProperty("webdriver.chrome.driver", envChromeDriverPath);
        String proxyAddress = System.getenv("PROXY_ADDRESS");

        if(options == null) {
            options = new ChromeOptions();
        }
        if (headlessMode) {
            options.addArguments("--no-sandbox");
            options.addArguments("headless");
            options.addArguments("--disable-gpu");
            options.addArguments("disable-infobars");
            options.addArguments("--disable-extensions");
            options.addArguments("window-size=1200x600");
        } else {
            options.addArguments("start-maximized");
            Logger.getGlobal().severe("WARNING: Headless Mode is deactivated, program will crash on headless linux");
        }
        options.setBinary(envChromeBinaryPath);

        if (proxyAddress != null && !proxyAddress.isEmpty()) {
            options.addArguments("--proxy-server=http://" + proxyAddress);
        }

        return new ChromeDriver(options);
    }
}
