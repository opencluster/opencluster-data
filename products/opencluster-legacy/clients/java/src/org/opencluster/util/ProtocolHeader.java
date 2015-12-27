package org.opencluster.util;

import java.nio.ByteBuffer;

/**
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

    private ProtocolCommand command = ProtocolCommand.NO_COMMAND;

    private ProtocolCommand replyToCommand = ProtocolCommand.NO_COMMAND;

    private int userSpecifiedID = 0;

    private int dataLength = 0;

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

    public ProtocolHeader() {
        // empty constructor for reading data.
    }

    public int readFromByteBuffer(ByteBuffer buf) {
        int startPos = buf.position();
        command = ProtocolCommand.fromCode(buf.getShort());
        replyToCommand = ProtocolCommand.fromCode(buf.getShort());
        userSpecifiedID = buf.getInt();
        dataLength = buf.getInt();
        int endPos = buf.position();
        return (endPos - startPos);
    }

    public int writeToByteBuffer(ByteBuffer buf) {
        int startPos = buf.position();
        buf.putShort(command.getCode());
        buf.putShort(replyToCommand.getCode());
        buf.putInt(userSpecifiedID);
        buf.putInt(dataLength);
        int endPos = buf.position();
        return (endPos - startPos);
    }

    public ProtocolCommand getCommand() {
        return command;
    }

    public void setCommand(ProtocolCommand command) {
        this.command = command;
    }

    public ProtocolCommand getReplyToCommand() {
        return replyToCommand;
    }

    public void setReplyToCommand(ProtocolCommand replyToCommand) {
        this.replyToCommand = replyToCommand;
    }

    public int getUserSpecifiedID() {
        return userSpecifiedID;
    }

    public void setUserSpecifiedID(int userSpecifiedID) {
        this.userSpecifiedID = userSpecifiedID;
    }

    public int getDataLength() {
        return dataLength;
    }

    public void setDataLength(int dataLength) {
        this.dataLength = dataLength;
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

