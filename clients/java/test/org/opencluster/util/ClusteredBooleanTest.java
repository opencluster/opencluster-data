package org.opencluster.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * User: Brian Gorrie
 * Date: 15/12/11
 * Time: 5:30 PM
 *
 * Test scenario for clustered boolean.
 *
 */
public class ClusteredBooleanTest {

    @Test
    public void TestClassCreation() {
        Assert.assertTrue("Class exists", new ClusteredBoolean("some-id",false) != null);
    }

}
