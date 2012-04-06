package org.opencluster.client;

import org.opencluster.util.HashMask;
import org.opencluster.util.LogUtil;
import org.opencluster.util.ProtocolCommand;
import org.opencluster.util.ProtocolHeader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.logging.Level;
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
        LOG.finest("Class created.");
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
                LOG.finest("Interrupted exception occurred.");
            }
        }

        Set<SocketOption<?>> socketOptions = sChannel.supportedOptions();
        for(SocketOption<?> socketOption : socketOptions ) {
            LOG.info("Supported option: " + socketOption.toString() +", Current Value: " + sChannel.getOption(socketOption));
        }

        return sChannel;
    }


    private void readFromSocketChannel(SocketChannel sChannel) {
        int numBytesRead = 0;
        do {
            ByteBuffer buf = ByteBuffer.allocateDirect(recieveBufferSize);

            try {
                // Clear the buffer and read bytes from socket
                buf.clear();

                numBytesRead = sChannel.read(buf);

                if (numBytesRead == -1) {
                    // No more bytes can be read from the channel
                    // do nothing
                    LOG.finest("No more data to read from channel.");
                } else {

                    // To read the bytes, flip the buffer
                    buf.flip();

                    // Read the bytes from the buffer ...;
                    // see Getting Bytes from a ByteBuffer
                    buf.rewind();

                    if (buf.limit() > 0) {

                        LOG.finest("Read data from Socket Channel");

                        while (buf.limit() - buf.position() >= 12) {
                            ProtocolHeader header = new ProtocolHeader();
                            int bytesRead = header.readFromByteBuffer(buf);
                            LOG.finest("Read in " + bytesRead + " bytes.");
                            LOG.finest(header.toString());
                            LogUtil.logByteBuffer(LOG, Level.FINEST, buf,buf.position() - bytesRead, buf.limit());

                            if( ProtocolCommand.HASH_MASK.equals(header.getCommand())) {
                                if(buf.limit() - buf.position() >= header.getDataLength()) {
                                    HashMask hashMask = new HashMask();
                                    bytesRead = hashMask.readFromByteBuffer(buf);
                                    LOG.finest("Read in " + bytesRead + " bytes.");
                                    LOG.finest(hashMask.toString());
                                    LogUtil.logByteBuffer(LOG, Level.FINEST, buf,buf.position() - bytesRead, buf.limit());

                                } else {
                                    LOG.finest("Not enough data available to read in hash mask.  Needed " + header.getDataLength() + " bytes. Found " + (buf.limit() - buf.position()) + " bytes.");
                                }
                            }
                        }

                        int remaining = buf.limit() - (buf.position() + 1);
                        if (remaining > 0) {
                            LOG.finest("There is unread data remaining: " + remaining + " bytes.");
                            LogUtil.logByteBuffer(LOG, Level.FINEST, buf, buf.position(), buf.limit());
                        }
                    }
                }
            } catch (IOException e) {
                LOG.finest("IO Exception occurred.");
                e.printStackTrace();
            }
        } while (numBytesRead > 0);
    }

    private void writeHeaderToConnection(SocketChannel sChannel, ProtocolHeader header) {
        ByteBuffer buf = ByteBuffer.allocateDirect(sendBufferSize);

        try {
            // Fill the buffer with the bytes to write;
            // see Putting Bytes into a ByteBuffer
            header.writeToByteBuffer(buf);

            // Prepare the buffer for reading by the socket
            buf.flip();

            LOG.finest("Bytes in buffer: " + buf.limit());

            // Write bytes
            int numBytesWritten = sChannel.write(buf);

            LOG.finest("Bytes Written To Socket: " + numBytesWritten);

            LogUtil.logByteBuffer(LOG, Level.FINEST, buf);

        } catch (IOException e) {
            LogUtil.logSevereException(LOG, "Exception occurred while attempting to write header to output stream.",e);
        }
    }


}
