package org.opencluster.util;

/**
 * User: Brian Gorrie
 * Date: 15/12/11
 * Time: 5:23 PM
 *
 * This class represents a clustered boolean value (true/false) that is atomic within a JVM.
 *
 */
public class ClusteredBoolean {

    private String clusterId;
    private boolean initialValue;

    public ClusteredBoolean( String clusterId, boolean initialValue ) {
        this.clusterId = clusterId;
        this.initialValue = initialValue;
    }

}
