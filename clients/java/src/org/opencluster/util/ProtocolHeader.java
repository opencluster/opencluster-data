package org.opencluster.util;

import java.nio.ByteBuffer;

/**
 * Created by IntelliJ IDEA.
 * User: Brian Gorrie
 * Date: 3/01/12
 * Time: 1:16 PM
 * <p/>
 * All communications begin with an 12-byte header.
 * ---------------------------------------------------------------------------------
 * |   Short Int   | 2 bytes |  Command (or Reply) Identifier                      |
 * |   Short Int   | 2 bytes |  Command being replied to (0 if not a reply)        |
 * |   Integer     | 4 bytes |  User specified ID                                  |
 * |   Integer     | 4 bytes |  Length of the data following this header           |
 * ---------------------------------------------------------------------------------
 */
public class ProtocolHeader {

    ProtocolCommand command = ProtocolCommand.NO_COMMAND;

    ProtocolCommand replyToCommand = ProtocolCommand.NO_COMMAND;

    int userSpecifiedID = 0;

    int dataLength = 0;

    public ProtocolHeader(ProtocolCommand command) {
        this.command = command;
    }

    public ProtocolHeader(ProtocolCommand command, ProtocolCommand replyToCommand) {
        this.command = command;
        this.replyToCommand = replyToCommand;
    }

    public ProtocolHeader(ProtocolCommand command, ProtocolCommand replyToCommand, int userSpecifiedID, int dataLength) {
        this.command = command;
        this.replyToCommand = replyToCommand;
        this.userSpecifiedID = userSpecifiedID;
        this.dataLength = dataLength;
    }

    public void writeToByteBuffer(ByteBuffer buf) {
        buf.putInt(dataLength);
        buf.putInt(userSpecifiedID);
        buf.putShort(replyToCommand.getCode());
        buf.putShort(command.getCode());
    }

    @Override
    public String toString() {
        return "ProtocolHeader{" +
                "command=" + command +
                ", replyToCommand=" + replyToCommand +
                ", userSpecifiedID=" + userSpecifiedID +
                ", dataLength=" + dataLength +
                '}';
    }

}

