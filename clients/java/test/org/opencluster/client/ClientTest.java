package org.opencluster.client;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * User: Brian Gorrie
 * Date: 1/04/12
 * Time: 9:43 PM
 */
public class ClientTest {

    private final static Logger LOG = Logger.getLogger(ClientTest.class.getName());

    public static final String HOST_NAME_OR_IP = "localhost";
    public static final int PORT = 7777;

    @Test
    public void TestClassExists() {
        Assert.assertTrue("Client Exists.", new Client() != null);

    }

    @Test
    public void TestClient() {
        setupLogLevel();
        Client client = new Client();
        client.addServer(HOST_NAME_OR_IP, PORT);
        try {
            client.start();
        } catch (IOException e) {
            logException(e, "Unable to start the client.");
        }
    }

    private void logException(Throwable e, String msg) {
        LOG.severe(msg);
        LOG.severe(ExceptionUtils.getMessage(e));
        LOG.severe(ExceptionUtils.getStackTrace(e));
        LOG.severe(ExceptionUtils.getRootCauseMessage(e));
        LOG.severe(ExceptionUtils.getStackTrace(ExceptionUtils.getRootCause(e)));
    }

    private void setupLogLevel() {
        LOG.info("Setting logging to log everything.");
        Enumeration<String> loggerNames = LogManager.getLogManager().getLoggerNames();
        for(; loggerNames.hasMoreElements();) {
            String loggerName = loggerNames.nextElement();
            LogManager.getLogManager().getLogger(loggerName).setLevel(Level.ALL);
        }
        LOG.info("Log level has now been set up.");
    }

}
