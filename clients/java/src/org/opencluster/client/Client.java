package org.opencluster.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.logging.Logger;

/**
 * User: Brian Gorrie
 * Date: 1/04/12
 * Time: 9:33 PM
 * <p/>
 * This class is a client fo the cluster.
 */
public class Client {

    private final static Logger LOG = Logger.getLogger(Client.class.getName());

    private String hostNameOrIP;
    private int port;
    private SocketChannel connection;
    private int sendBufferSize;
    private int recieveBufferSize;

    public void Client() {
        // Empty constructor
    }

    public void start() throws IOException {
        // start the client.
        connection = createConnection();
    }

    public void addServer(String hostNameOrIP, int port) {
        this.hostNameOrIP = hostNameOrIP;
        this.port = port;
    }

    private SocketChannel createConnection() throws IOException {
        SocketChannel sChannel = null;
        sChannel = SocketChannel.open();
        sChannel.configureBlocking(false);

        sendBufferSize = sChannel.getOption(StandardSocketOptions.SO_SNDBUF);
        LOG.info("Using send buffer size: " + sendBufferSize);
        recieveBufferSize = sChannel.getOption(StandardSocketOptions.SO_RCVBUF);
        LOG.info("Using recieve buffer size: " + recieveBufferSize);

        // Send a connection request to the server; this method is non-blocking
        sChannel.connect(new InetSocketAddress(hostNameOrIP, port));

        while (!sChannel.finishConnect()) {
            LOG.info("Not connected yet, waiting for connection to complete.");
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                //
            }
        }

        Set<SocketOption<?>> socketOptions = sChannel.supportedOptions();
        for(SocketOption<?> socketOption : socketOptions ) {
            LOG.info("Supported option: " + socketOption.toString() +", Current Value: " + sChannel.getOption(socketOption));
        }

        return sChannel;
    }

}
