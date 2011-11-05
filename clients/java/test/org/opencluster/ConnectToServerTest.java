package org.opencluster;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
 * Created by IntelliJ IDEA.
 * User: Brian Gorrie
 * Date: 5/11/11
 * Time: 8:09 PM
 * <p/>
 * Example of connecting to server.
 */
public class ConnectToServerTest {

    @Test
    public void canIConnectToServer() {

        // now on my machine it is connecting to localhost:7777 due to how I am plugging virtual box into my network.
        // Here is the command I use to start ocd on the ubuntu virtual box instance
        // ocd -l $(ifconfig | grep "inet addr" | head -n 1 | sed 's/:/ /g' | awk '{print $3}'):7777 -vvv

        // Create a non-blocking socket channel
        SocketChannel sChannel = null;
        try {
            sChannel = SocketChannel.open();
            sChannel.configureBlocking(false);

            // Send a connection request to the server; this method is non-blocking
            sChannel.connect(new InetSocketAddress("localhost", 7777));

            sChannel.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        Assert.assertTrue("Unable to create connection through to server, double check it is running.", sChannel != null);
    }

    @Test
    public void doesServerDetectUngracefulDisconnect() {

        // now on my machine it is connecting to localhost:7777 due to how I am plugging virtual box into my network.
        // Here is the command I use to start ocd on the ubuntu virtual box instance
        // ocd -l $(ifconfig | grep "inet addr" | head -n 1 | sed 's/:/ /g' | awk '{print $3}'):7777 -vvv

        // Create a non-blocking socket channel
        SocketChannel sChannel = null;
        try {
            sChannel = SocketChannel.open();
            sChannel.configureBlocking(false);

            // Send a connection request to the server; this method is non-blocking
            sChannel.connect(new InetSocketAddress("localhost", 7777));

            // don't close the channel
            // and then exit ...
            //sChannel.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        Assert.assertTrue("Unable to create connection through to server, double check it is running.", sChannel != null);
    }

}
