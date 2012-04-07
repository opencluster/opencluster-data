package org.opencluster.util;

import java.nio.ByteBuffer;

/**
 * User: Brian Gorrie
 * Date: 1/04/12
 * Time: 9:41 AM
 * Represents a hash mask sent from the server to the client.
 *  integer - mask
 *  integer - hash
 *  integer - instance count (-1 = not hosted, 0 = primary, 1 or more = backup).

 */
public class HashMask {
    private int mask;
    private int hash;
    private int instanceCount;

    public int readFromByteBuffer(ByteBuffer buf) {
        int startPos = buf.position();
        mask = buf.getInt();
        hash = buf.getInt();
        instanceCount = buf.getInt();
        int endPos = buf.position();
        return (endPos - startPos);
    }

    public int writeToByteBuffer(ByteBuffer buf) {
        int startPos = buf.position();
        buf.putInt(mask);
        buf.putInt(hash);
        buf.putInt(instanceCount);
        int endPos = buf.position();
        return (endPos - startPos);
    }


    public int getMask() {
        return mask;
    }

    public void setMask(int mask) {
        this.mask = mask;
    }

    public int getHash() {
        return hash;
    }

    public void setHash(int hash) {
        this.hash = hash;
    }

    public int getInstanceCount() {
        return instanceCount;
    }

    public void setInstanceCount(int instanceCount) {
        this.instanceCount = instanceCount;
    }

    @Override
    public String toString() {
        return "HashMask{" +
                "mask=" + mask +
                ", hash=" + hash +
                ", instanceCount=" + instanceCount +
                '}';
    }

}
