package org.opencluster.util;

/**
 * Created by IntelliJ IDEA.
 * User: Brian Gorrie
 * Date: 3/01/12
 * Time: 1:19 PM
 * The commands are:
 * 10    hello         initiate communications.
 * 11    capabilities  determine if a command is accepted or not, without issuing the command.
 * 15    shuttingdown  tells the other node that this node is shutting down and not to send any
 * buckets to it, or other data.
 * 20    goodbye       terminate communications cleanly
 * 30    ping          check that the node is responding.
 * 50    serverhello   when a server is connecting to another server.
 * 100   server_info   sends a list of servers in the cluster.
 * 120   hashmask      when a set of hashes is being moved to a different server, let the clients and
 * other servers know.
 * 200   req_loadlevel nodes will ask other nodes how full they are.
 * 300   accept_bucket node telling another node to start accepting data for a bucket.
 * 1000  lock          obtain a cluster lock
 * 1010  unlock        release a cluster lock
 * 2000  set_int       set an integer (32-bit) key/value, overwriting if it exists.
 * 2010  set_long      set an long (64-bit) key/value, overwriting it it exists.
 * 2020  set_string    set a variable length string key/value.
 * 2100  get_int       get a integer (32-bit) key/value pair.
 * 2110  get_long      get a long (32-bit) key/value pair.
 * 2120  get_string    get a variable length string key/value.
 * 2200  get_type      get the type of the data.
 */
public enum ProtocolCommand {

    NO_COMMAND(0),
    HELLO(10),
    CAPABILITIES(11),
    SHUTTING_DOWN(15),
    GOODBYE(20),
    PING(30),
    SERVER_HELLO(50),
    SERVER_INFO(100),
    HASH_MASK(120),
    REQ_LOAD_LEVEL(200),
    ACCEPT_BUCKET(300),
    LOCK(1000),
    UNLOCK(1010),
    SET_INT(2000),
    SET_LONG(2010),
    SET_STRING(2020),
    GET_INT(2100),
    GET_LONG(2110),
    GET_STRING(2120),
    GET_TYPE(2200);

    private short code;

    ProtocolCommand(int code) {
        this.code = (short)code;
    }

    public short getCode() {
        return code;
    }

}
