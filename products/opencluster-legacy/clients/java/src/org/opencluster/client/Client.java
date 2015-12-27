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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
    private int receiveBufferSize;

    private ExecutorService readingThread = Executors.newSingleThreadExecutor();

    public void Client() {
        LOG.info("Class created.");
    }

    public void start() throws IOException {
        // start the client.
        connection = createConnection();

        ProtocolHeader header = new ProtocolHeader(ProtocolCommand.HELLO);

        LOG.info(header.toString());

        writeHeaderToConnection(connection, header);

        readingThread.submit(new Runnable() {
            @Override
            public void run() {
                boolean exitLoop = false;
                for (; !exitLoop && !readingThread.isShutdown() && !readingThread.isTerminated(); ) {
                    try {
                        readFromSocketChannel(connection);
                        Thread.sleep(50L);
                    } catch (InterruptedException t) {
                        LOG.info("Thread interrupted.");
                        exitLoop = true;
                    } catch (ClosedChannelException c) {
                        LOG.info("Connection to server closed.");
                        exitLoop = true;
                    } catch (Throwable t) {
                        LogUtil.logSevereException(LOG, "Exception reading data.", t);
                        try {
                            Thread.sleep(2000L);
                        } catch (InterruptedException e) {
                            //
                        }
                    }
                }
            }
        });

    }

    public void stop() throws IOException {
        ProtocolHeader header = new ProtocolHeader(ProtocolCommand.GOODBYE);

        LOG.info(header.toString());

        writeHeaderToConnection(connection, header);

        // Allow the server time to respond.
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            LOG.info("Thread interrupted.");
        }

        readingThread.shutdown();

        try {
            readingThread.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.info("Thread interrupted.");
        }

        connection.close();

        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            LOG.info("Thread interrupted.");
        }

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
        receiveBufferSize = sChannel.getOption(StandardSocketOptions.SO_RCVBUF);
        LOG.info("Using receive buffer size: " + receiveBufferSize);

        // Send a connection request to the server; this method is non-blocking
        sChannel.connect(new InetSocketAddress(hostNameOrIP, port));

        while (!sChannel.finishConnect()) {
            LOG.info("Not connected yet, waiting for connection to complete.");
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                LOG.info("Interrupted exception occurred.");
            }
        }

        Set<SocketOption<?>> socketOptions = sChannel.supportedOptions();
        for (SocketOption<?> socketOption : socketOptions) {
            LOG.info("Supported option: " + socketOption.toString() + ", Current Value: " + sChannel.getOption(socketOption));
        }

        return sChannel;
    }


    private void readFromSocketChannel(SocketChannel sChannel) throws IOException {
        int numBytesRead = 0;
        do {
            ByteBuffer buf = ByteBuffer.allocateDirect(receiveBufferSize);

            // Clear the buffer and read bytes from socket
            buf.clear();

            numBytesRead = sChannel.read(buf);

            if (numBytesRead > 0) {

                // To read the bytes, flip the buffer
                buf.flip();

                // Read the bytes from the buffer ...;
                // see Getting Bytes from a ByteBuffer
                buf.rewind();

                if (buf.limit() > 0) {

                    LOG.info("Read data from Socket Channel");

                    while (buf.limit() - buf.position() >= 12) {
                        ProtocolHeader header = new ProtocolHeader();
                        int bytesRead = header.readFromByteBuffer(buf);
                        LOG.info("Read in " + bytesRead + " bytes.");
                        LOG.info(header.toString());
                        LogUtil.logByteBuffer(LOG, Level.INFO, buf, buf.position() - bytesRead, buf.position());

                        if (ProtocolCommand.HASH_MASK.equals(header.getCommand())) {
                            if (buf.limit() - buf.position() >= header.getDataLength()) {
                                HashMask hashMask = new HashMask();
                                bytesRead = hashMask.readFromByteBuffer(buf);
                                LOG.info("Read in " + bytesRead + " bytes.");
                                LOG.info(hashMask.toString());
                                LogUtil.logByteBuffer(LOG, Level.INFO, buf, buf.position() - bytesRead, buf.position());

                            } else {
                                LOG.info("Not enough data available to read in hash mask.  Needed " + header.getDataLength() + " bytes. Found " + (buf.limit() - buf.position()) + " bytes.");
                            }
                        }
                    }

                    int remaining = buf.limit() - (buf.position() + 1);
                    if (remaining > 0) {
                        LOG.info("There is unread data remaining: " + remaining + " bytes.");
                        LogUtil.logByteBuffer(LOG, Level.INFO, buf, buf.position(), buf.limit());
                    }
                }
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

            LOG.info("Bytes in buffer: " + buf.limit());

            // Write bytes
            int numBytesWritten = sChannel.write(buf);

            LOG.info("Bytes Written To Socket: " + numBytesWritten);

            LogUtil.logByteBuffer(LOG, Level.INFO, buf, 0, numBytesWritten);

        } catch (IOException e) {
            LogUtil.logSevereException(LOG, "Exception occurred while attempting to write header to output stream.", e);
        }
    }


}
