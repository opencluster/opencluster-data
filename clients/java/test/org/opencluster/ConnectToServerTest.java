package org.opencluster;

import org.junit.Assert;
import org.junit.Test;
import org.opencluster.util.ProtocolCommand;
import org.opencluster.util.ProtocolHeader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * User: Brian Gorrie
 * Date: 5/11/11
 * Time: 8:09 PM
 * <p/>
 * Example of connecting to server.
 *
 * Note: when testing always run with the -enableassertions or -ea for the jdk so that any
 * native java assertions are executed as well.  IntelliJ does this by default for you.
 *
 */
public class ConnectToServerTest {

    // The IP of the server you are connecting to - will be in the logs of the server when it starts up.
    // If running server in a virtual box guest use the bridged network
    // Also add the ip and hostname to your hosts file
    // # ip address of vbox image.
    // 192.168.0.14  vbox-image

    private static final String HOST_NAME = "vbox-image";
    //private static final String HOST_IP = "192.168.0.14";

    private static final int PORT = 7777;

    @Test
    public void canIConnectToServer() {

        // now on my machine it is connecting to localhost:7777 due to how I am plugging virtual box into my network.
        // Here is the command I use to start ocd on the ubuntu virtual box instance
        // ocd -l $(ifconfig | grep "inet addr" | head -n 1 | sed 's/:/ /g' | awk '{print $3}'):7777 -vvv

        // Create a non-blocking socket channel
        SocketChannel sChannel = null;
        try {
            sChannel = SocketChannel.open();

            Assert.assertTrue("Unable to create connection through to server, double check it is running.", sChannel != null);

            sChannel.configureBlocking(false);

            Assert.assertTrue("Unable to set blocking to false.", !sChannel.isBlocking());

            // Send a connection request to the server; this method is non-blocking
            sChannel.connect(new InetSocketAddress(HOST_NAME, PORT));

            int attempts = 0;
            while(!sChannel.finishConnect() && attempts < 20) {
                // Waiting for the connection to complete.
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // ignore this exception and truck on.
                }
                // Wait for 20 seconds before giving up the ghost.
                attempts++;
            }

            // finish connect needs to be checked before isConnected
            Assert.assertTrue("Socket did not finish connection.", sChannel.finishConnect());

            // is connected will return false if called prior to finishConnect
            Assert.assertTrue("Socket did not connect to Server.", sChannel.isConnected());

            sChannel.close();

            Assert.assertTrue("Socket did not close correctly.", !sChannel.isConnected());

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
            System.out.println("SO_LINGER: " + sChannel.getOption(StandardSocketOptions.SO_LINGER));

            // Send a connection request to the server; this method is non-blocking
            sChannel.connect(new InetSocketAddress(HOST_NAME, PORT));

            // don't close the channel
            // and then exit ...
            //sChannel.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        Assert.assertTrue("Unable to create connection through to server, double check it is running.", sChannel != null);
    }

    @Test
    public void doesServerDetectUngracefulDisconnectWithSOLingerSet() {

        // now on my machine it is connecting to localhost:7777 due to how I am plugging virtual box into my network.
        // Here is the command I use to start ocd on the ubuntu virtual box instance
        // ocd -l $(ifconfig | grep "inet addr" | head -n 1 | sed 's/:/ /g' | awk '{print $3}'):7777 -vvv

        // Create a non-blocking socket channel
        SocketChannel sChannel = null;
        try {

            sChannel = SocketChannel.open();
            sChannel.configureBlocking(false);

            System.out.println("Before SO_LINGER: " + sChannel.getOption(StandardSocketOptions.SO_LINGER));
            sChannel.setOption(StandardSocketOptions.SO_LINGER, 0);
            System.out.println("After SO_LINGER: " + sChannel.getOption(StandardSocketOptions.SO_LINGER));


            // Send a connection request to the server; this method is non-blocking
            sChannel.connect(new InetSocketAddress(HOST_NAME, PORT));

            // don't close the channel
            // and then exit ...
            //sChannel.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        Assert.assertTrue("Unable to create connection through to server, double check it is running.", sChannel != null);
    }

    @Test
    public void doesServerDetectUngracefulDisconnectWaitForFinishedConnect() {

        // now on my machine it is connecting to localhost:7777 due to how I am plugging virtual box into my network.
        // Here is the command I use to start ocd on the ubuntu virtual box instance
        // ocd -l $(ifconfig | grep "inet addr" | head -n 1 | sed 's/:/ /g' | awk '{print $3}'):7777 -vvv

        // Create a non-blocking socket channel
        SocketChannel sChannel = null;
        try {
            sChannel = SocketChannel.open();
            sChannel.configureBlocking(false);
            System.out.println("SO_LINGER: " + sChannel.getOption(StandardSocketOptions.SO_LINGER));

            // Send a connection request to the server; this method is non-blocking
            sChannel.connect(new InetSocketAddress(HOST_NAME, PORT));

            while (!sChannel.finishConnect()) {
                System.out.println("Not connected yet, waiting for connection to complete.");
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        Assert.assertTrue("Unable to create connection through to server, double check it is running.", sChannel != null);
    }

    @Test
    public void ConnectAndSendHello() {

        // now on my machine it is connecting to localhost:7777 due to how I am plugging virtual box into my network.
        // Here is the command I use to start ocd on the ubuntu virtual box instance
        // ocd -l $(ifconfig | grep "inet addr" | head -n 1 | sed 's/:/ /g' | awk '{print $3}'):7777 -vvv

        // Create a non-blocking socket channel
        SocketChannel sChannel = null;
        try {
            sChannel = SocketChannel.open();
            sChannel.configureBlocking(false);
            System.out.println("SO_LINGER: " + sChannel.getOption(StandardSocketOptions.SO_LINGER));

            // Send a connection request to the server; this method is non-blocking
            sChannel.connect(new InetSocketAddress(HOST_NAME, PORT));

            while (!sChannel.finishConnect()) {
                System.out.println("Not connected yet, waiting for connection to complete.");
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            ProtocolHeader header = new ProtocolHeader(ProtocolCommand.HELLO);

            ByteBuffer buf = ByteBuffer.allocateDirect(1024);

            try {
                // Fill the buffer with the bytes to write;
                // see Putting Bytes into a ByteBuffer
                header.writeToByteBuffer(buf);

                // Prepare the buffer for reading by the socket
                buf.flip();

                // Write bytes
                int numBytesWritten = sChannel.write(buf);

                Assert.assertTrue("No data was written to socket channel.", numBytesWritten > 0);
                System.out.println("Bytes Written: " + numBytesWritten);

            } catch (IOException e) {
                e.printStackTrace();
                Assert.assertTrue("Exception occurred writing to socket: " + e.getMessage(),false);
            }

            sChannel.close();

        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertTrue("Exception occurred: " + e.getMessage(), false);
        }

    }

}
