package org.opencluster;

import org.junit.Assert;
import org.junit.Test;
import org.opencluster.util.HashMask;
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
 */
public class ProtocolTest {

    private static final String HOSTNAME = "localhost";
    private static final int PORT = 7777;


    @Test
    public void ConnectAndSendHello() {

        // now on my machine it is connecting to localhost:7777 due to how I am plugging virtual box into my network.
        // Here is the command I use to start ocd on the ubuntu virtual box instance
        // ocd -l $(ifconfig | grep "inet addr" | head -n 1 | sed 's/:/ /g' | awk '{print $3}'):7777 -vvv

        // Create a non-blocking socket channel
        SocketChannel sChannel = createConnection();

        ProtocolHeader header = new ProtocolHeader(ProtocolCommand.HELLO);

        writeHeaderToConnection(sChannel, header);

        readFromSocketChannel(sChannel);

        closeConnection(sChannel);

    }

    @Test
    public void ConnectAndSendHelloThenGoodBye() {

        // now on my machine it is connecting to localhost:7777 due to how I am plugging virtual box into my network.
        // Here is the command I use to start ocd on the ubuntu virtual box instance
        // ocd -l $(ifconfig | grep "inet addr" | head -n 1 | sed 's/:/ /g' | awk '{print $3}'):7777 -vvv

        // Create a non-blocking socket channel
        SocketChannel sChannel = createConnection();

        ProtocolHeader header = new ProtocolHeader(ProtocolCommand.HELLO);

        System.out.println(header.toString());

        writeHeaderToConnection(sChannel, header);

        try {
            System.out.println("Pausing for 5 seconds");
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
            //
        }

        readFromSocketChannel(sChannel);

        // Good bye is no longer supported
//        header = new ProtocolHeader(ProtocolCommand.GOODBYE);
//
//        writeHeaderToConnection(sChannel, header);
//
//        try {
//            System.out.println("Pausing for 5 seconds");
//            Thread.sleep(5000L);
//        } catch (InterruptedException e) {
//            //
//        }
//
//        readFromSocketChannel(sChannel);
//
//        try {
//            System.out.println("Pausing for 5 seconds");
//            Thread.sleep(5000L);
//        } catch (InterruptedException e) {
//            //
//        }

        System.out.println("Closing the connection.");
        closeConnection(sChannel);

    }

    private void readFromSocketChannel(SocketChannel sChannel) {
        // Create a direct buffer to get bytes from socket.
        // Direct buffers should be long-lived and be reused as much as possible.
        int numBytesRead = 0;
        do {
            ByteBuffer buf = ByteBuffer.allocateDirect(1024);

            try {
                // Clear the buffer and read bytes from socket
                buf.clear();
                numBytesRead = sChannel.read(buf);

                // Get the ByteBuffer's capacity
                int capacity = buf.capacity(); // 10


                if (numBytesRead == -1) {
                    // No more bytes can be read from the channel
                    // do nothing
                    System.out.println("No more data to read from channel.");
                } else {

                    // To read the bytes, flip the buffer
                    buf.flip();

                    // Read the bytes from the buffer ...;
                    // see Getting Bytes from a ByteBuffer
                    buf.rewind();

                    if (buf.limit() > 0) {

                        System.out.println();
                        System.out.println("Read data from Socket Channel");

                        while (buf.limit() - buf.position() >= 12) {
                            ProtocolHeader header = new ProtocolHeader();
                            int bytesRead = header.readFromByteBuffer(buf);
                            System.out.println("Read in " + bytesRead + " bytes.");
                            System.out.println(header.toString());
                            for (int i = (buf.position() - bytesRead); i < buf.position(); i++) {
                                System.out.print("[" + buf.get(i) + "]");
                            }
                            System.out.println();
                            if( ProtocolCommand.HASH_MASK.equals(header.getCommand())) {
                                if(buf.limit() - buf.position() >= header.getDataLength()) {
                                    HashMask hashMask = new HashMask();
                                    bytesRead = hashMask.readFromByteBuffer(buf);
                                    System.out.println("Read in " + bytesRead + " bytes.");
                                    System.out.println(hashMask.toString());
                                    for (int i = (buf.position() - bytesRead); i < buf.position(); i++) {
                                        System.out.print("[" + buf.get(i) + "]");
                                    }
                                } else {
                                    System.out.println("Not enough data available to read in hash mask.  Needed " + header.getDataLength() + " bytes. Found " + (buf.limit() - buf.position()) + " bytes.");
                                }
                            }
                        }

                        int remaining = buf.limit() - (buf.position() + 1);
                        if (remaining > 0) {
                            System.out.println("There is unread data remaining: " + remaining + " bytes.");
                            for (int i = buf.position(); i < buf.limit(); i++) {
                                System.out.print("[" + buf.get(i) + "]");
                            }
                        }
                        System.out.println();
                    }
                }
            } catch (IOException e) {
                System.out.println("IO Exception occurred.");
                e.printStackTrace();
            }
        } while (numBytesRead > 0);
    }

    private void writeHeaderToConnection(SocketChannel sChannel, ProtocolHeader header) {
        ByteBuffer buf = ByteBuffer.allocateDirect(1024);

        try {
            // Fill the buffer with the bytes to write;
            // see Putting Bytes into a ByteBuffer
            header.writeToByteBuffer(buf);

            // Prepare the buffer for reading by the socket
            buf.flip();

            System.out.println("Bytes in buffer: " + buf.limit());

            // Write bytes
            int numBytesWritten = sChannel.write(buf);

            Assert.assertTrue("No data was written to socket channel.", numBytesWritten > 0);
            System.out.println("Bytes Written To Socket: " + numBytesWritten);
            System.out.println("buf:" + buf.toString());
            buf.rewind();
            byte[] data = new byte[12];
            buf.get(data);
            for (int i = 0; i < data.length; i++) {
                System.out.print("[" + data[i] + "]");
            }
            System.out.println();

        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertTrue("Exception occurred writing to socket: " + e.getMessage(), false);
        }
    }

    private void closeConnection(SocketChannel sChannel) {
        try {
            sChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertTrue("Exception occurred closing socket: " + e.getMessage(), false);
        }
    }

    private SocketChannel createConnection() {
        SocketChannel sChannel = null;
        try {
            sChannel = SocketChannel.open();
            sChannel.configureBlocking(false);
            System.out.println("SO_LINGER: " + sChannel.getOption(StandardSocketOptions.SO_LINGER));

            // Send a connection request to the server; this method is non-blocking
            sChannel.connect(new InetSocketAddress(HOSTNAME, PORT));

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
            Assert.assertTrue("Exception occurred: " + e.getMessage(), false);
        }

        Assert.assertTrue("Unable to create connection through to server, double check it is running.", sChannel != null);
        return sChannel;
    }

}
