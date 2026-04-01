/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.zk;

import org.apache.curator.test.TestingCluster;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Reproducer for IGNITE-27016.
 *
 * <p>This test starts the second node in a separate JVM and validates that
 * discovery SPI preprocessed to {@link ZookeeperDiscoverySpi} is preserved
 * through remote-node configuration serialization.</p>
 *
 * <p>Expected behavior:
 * with fix - node joins successfully;
 * without fix - remote startup fails (e.g. "Remote node has not joined").</p>
 */
public class ZookeeperDiscoveryMultiJvmNodeRunnerReproducerTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static TestingCluster testingCluster;

    /** */
    private static String oldZkForceSync;

    /** */
    private static String oldCfgPreprocessor;

    /** */
    @BeforeClass
    public static void beforeClass() throws Exception {
        oldZkForceSync = System.getProperty("zookeeper.forceSync");
        oldCfgPreprocessor = System.getProperty(GridTestProperties.IGNITE_CFG_PREPROCESSOR_CLS);

        System.setProperty("zookeeper.forceSync", "false");

        testingCluster = ZookeeperDiscoverySpiTestUtil.createTestingCluster(3);
        testingCluster.start();

        System.setProperty(
                GridTestProperties.IGNITE_CFG_PREPROCESSOR_CLS,
                ZookeeperDiscoveryMultiJvmNodeRunnerReproducerTest.class.getName()
        );
    }

    /** */
    @AfterClass
    public static void afterClass() throws Exception {
        if (testingCluster != null)
            testingCluster.close();

        testingCluster = null;

        if (oldZkForceSync != null)
            System.setProperty("zookeeper.forceSync", oldZkForceSync);
        else
            System.clearProperty("zookeeper.forceSync");

        if (oldCfgPreprocessor != null)
            System.setProperty(GridTestProperties.IGNITE_CFG_PREPROCESSOR_CLS, oldCfgPreprocessor);
        else
            System.clearProperty(GridTestProperties.IGNITE_CFG_PREPROCESSOR_CLS);
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Intentionally set TCP discovery in base config.
        // The preprocessor must replace it with ZooKeeper discovery.
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected Ignite startRemoteGrid(
            String igniteInstanceName,
            IgniteConfiguration cfg,
            GridSpringResourceContext ctx,
            boolean resetDiscovery
    ) throws Exception {
        if (ctx != null)
            throw new UnsupportedOperationException(
                    "Starting a grid in another JVM with Spring context is not supported."
            );

        if (cfg == null)
            cfg = optimize(getConfiguration(igniteInstanceName));

        // Critical for reproducer:
        // apply preprocessor before IgniteNodeRunner.storeToFile(...),
        // then force resetDiscovery=true to exercise serialization path.
        preprocessConfiguration(cfg);

        return new IgniteProcessProxy(
                cfg,
                cfg.getGridLogger(),
                () -> grid(0),
                true,
                additionalRemoteJvmArgs()
        );
    }

    /**
     * Called via reflection by {@link org.apache.ignite.testframework.junits.GridAbstractTest}.
     *
     * @param cfg Configuration to preprocess.
     */
    @SuppressWarnings("unused")
    public static void preprocessConfiguration(IgniteConfiguration cfg) {
        if (testingCluster == null)
            throw new IllegalStateException("Test ZooKeeper cluster is not started.");

        ZookeeperDiscoverySpi zkSpi = new TestZookeeperDiscoverySpi();

        DiscoverySpi spi = cfg.getDiscoverySpi();

        if (spi instanceof TcpDiscoverySpi)
            zkSpi.setClientReconnectDisabled(((TcpDiscoverySpi)spi).isClientReconnectDisabled());

        zkSpi.setSessionTimeout(30_000);
        zkSpi.setZkConnectionString(testingCluster.getConnectString());

        cfg.setDiscoverySpi(zkSpi);
    }

    /**
     * Reproduces the bug from unpatched code path: remote node startup fails.
     */
    @Test
    public void testRemoteNodeStartWithPreprocessedDiscovery() throws Exception {
        Ignite n0 = startGrid(0);

        assertTrue(n0.configuration().getDiscoverySpi() instanceof ZookeeperDiscoverySpi);

        Ignite n1 = startGrid(1);

        assertTrue(isMultiJvmObject(n1));
        assertTrue(n1.configuration().getDiscoverySpi() instanceof ZookeeperDiscoverySpi);

        assertTrue(
                "Topology did not reach 2 nodes in time",
                GridTestUtils.waitForCondition(() -> n0.cluster().nodes().size() == 2, 30_000)
        );
    }
}
