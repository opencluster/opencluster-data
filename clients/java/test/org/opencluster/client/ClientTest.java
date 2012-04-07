package org.opencluster.client;

import org.junit.Assert;
import org.junit.Test;
import org.opencluster.util.LogUtil;

import java.io.IOException;
import java.util.logging.Level;
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
        LogUtil.setupDefaultLogLevel(LOG, Level.FINEST);
        Client client = new Client();
        client.addServer(HOST_NAME_OR_IP, PORT);
        try {
            client.start();
            try {
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
                //
            }
            client.stop();
        } catch (IOException e) {
            LogUtil.logSevereException(LOG, "Unable to start the client.", e);
        }
    }

}
