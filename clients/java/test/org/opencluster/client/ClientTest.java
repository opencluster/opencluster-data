package org.opencluster.client;

import org.junit.Assert;
import org.junit.Test;

/**
 * User: Brian Gorrie
 * Date: 1/04/12
 * Time: 9:43 PM
 */
public class ClientTest {

    private static final String HOSTNAME = "localhost";
    private static final int PORT = 7777;

    @Test
    public void TestClassExists() {
        Assert.assertTrue("Client Exists.", new Client() != null);

    }

}
