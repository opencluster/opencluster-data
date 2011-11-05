package org.opencluster;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;

/**
 * Created by IntelliJ IDEA.
 * User: Brian Gorrie
 * Date: 5/11/11
 * Time: 10:24 PM
 * <p/>
 * This is to allow the test to run as a standalone app
 */
public class CreateConnectionAndExitWithoutClosing {

    public static void main(String[] args) {
        // now on my machine it is connecting to localhost:7777 due to how I am plugging virtual box into my network.
        // Here is the command I use to start ocd on the ubuntu virtual box instance
        // ocd -l $(ifconfig | grep "inet addr" | head -n 1 | sed 's/:/ /g' | awk '{print $3}'):7777 -vvv

        if (args.length < 2) {
            System.out.println("Specify ip and port");
        } else {

            // Create a non-blocking socket channel
            SocketChannel sChannel = null;
            try {
                sChannel = SocketChannel.open();
                sChannel.configureBlocking(false);
                System.out.println("SO_LINGER: " + sChannel.getOption(StandardSocketOptions.SO_LINGER));

                // Send a connection request to the server; this method is non-blocking
                sChannel.connect(new InetSocketAddress(args[0], Integer.parseInt(args[1])));

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
        }

    }

}
