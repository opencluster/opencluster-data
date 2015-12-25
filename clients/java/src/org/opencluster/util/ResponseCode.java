package org.opencluster.util;

/**
 * User: Brian Gorrie
 * Date: 3/01/12
 * Time: 1:19 PM
 *
 * For more information on the commands head to https://github.com/hyper/opencluster/wiki/Client-Protocol
 *
 */
public enum ResponseCode {

    RESPONSE_CODE_NOT_RECOGNISED(-1),
    //Handshake
    TRY_ELSEWHERE(0x0001),
    UNKNOWN(0x0002),
    FAIL(0x0003),
    NOT_EXIST(0x0004),
    WRONG_TYPE(0x0005),
    TOO_LARGE(0x0006),
    OK(0x0010),
    CONNECT_INFO(0x0011),
    SERVER_INFO(0x0012),
    LOAD_LEVELS(0x0013),
    COMMAND_CONTROL(0x0014),
    LOCK(0x0015),
    DATA_TYPE_INTEGER(0x0016),
    DATA_TYPE_STRING(0x0017),
    DATA_INTEGER(0x0018),
    DATA_STRING(0x0019),
    DATA_STRING_LENGTH(0x001A),
    KEY_VALUE_HASH(0x001B),
    KEY_VALUE(0x001C);

    private short code;

    ResponseCode(int code) {
        this.code = (short)code;
    }

    public short getCode() {
        return code;
    }

    public static ResponseCode fromCode(short code) {
        ResponseCode[] values = values();
        for (ResponseCode value : values) {
            if (value.getCode() == code) {
                return value;
            }
        }
        return RESPONSE_CODE_NOT_RECOGNISED;
    }

}
