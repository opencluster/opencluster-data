package org.opencluster.util;

/**
 * User: Brian Gorrie
 * Date: 3/01/12
 * Time: 1:19 PM
 *
 * For more information on the commands head to https://github.com/hyper/opencluster/wiki/Client-Protocol
 *
 */
public enum ProtocolCommand {

    UNKNOWN_COMMAND(-1),
    //Handshake
    NO_COMMAND(0),
    HELLO(0x0010),
    CAPABILITIES(0x0020),
    SHUTTING_DOWN(0x0030),
    GOODBYE(0x0040),
    PING(0x0050),
    REQUEST_SERVER_INFO(0x0070),
    HASH_MASK(0x0080),
    //Node Operations
    CONTROL(0x0200),
    SNAPSHOT_CONTROL(0x0210),
    //Locking
    LOCK(0x1000),
    LOCK_WAIT(0x1010),
    UNLOCK(0x1020),
    LOCK_TOUCH(0x1030),
    LOCK_EXPIRED(0x1040),
    //Subscription Events
    SUBSCRIBE(0x1100),
    UNSUBSCRIBE(0x1110),
    EVENT_ADDED(0x1200),
    EVENT_DELETED(0x1210),
    EVENT_MODIFIED(0x1220),
    EVENT_EXPIRED(0x1230),
    //Data Operations
    GET_TYPE(0x2000),
    GET_INT(0x2010),
    GET_STRING(0x2020),
    GET_STRING_SUBSTRING(0x2030),
    GET_STRING_LENGTH(0x2040),
    SET_INT(0x2200),
    SET_STRING(0x2210),
    SET_SUBSTRING(0x2220),
    DELETE(0x2400),
    SET_KEY_VALUE(0x2500),
    GET_KEY_VALUE(0x2520),
    DELETE_KEY_VALUE(0x2530),
    INT_INCREMENT(0x2600),
    INT_DECREMENT(0x2610);

    private short code;

    ProtocolCommand(int code) {
        this.code = (short)code;
    }

    public short getCode() {
        return code;
    }

    public static ProtocolCommand fromCode(short code) {
        ProtocolCommand[] values = values();
        for (ProtocolCommand value : values) {
            if (value.getCode() == code) {
                return value;
            }
        }
        return UNKNOWN_COMMAND;
    }

}
