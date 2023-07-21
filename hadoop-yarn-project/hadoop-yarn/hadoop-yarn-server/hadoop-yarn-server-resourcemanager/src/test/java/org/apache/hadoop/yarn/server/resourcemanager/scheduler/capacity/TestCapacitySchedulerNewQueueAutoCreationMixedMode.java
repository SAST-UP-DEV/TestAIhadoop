/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestCapacitySchedulerNewQueueAutoCreationMixedMode
  extends TestCapacitySchedulerAutoCreatedQueueBase {
  public static final int GB = 1024;
  private static final String NL = CommonNodeLabelsManager.NO_LABEL;
  private MockRM mockRM = null;
  private CapacityScheduler cs;
  private CapacitySchedulerConfiguration csConf;
  private CapacitySchedulerQueueManager autoQueueHandler;

  public CapacityScheduler getCs() {
    return cs;
  }

  /*
  Create the following structure:
           root
        /   |   \
      a     b    e
    /
  a1
   */
  @Before
  public void setUp() throws Exception {
    csConf = new CapacitySchedulerConfiguration();
    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    csConf.setLegacyQueueModeEnabled(false);
  }

  @After
  public void tearDown() {
    if (mockRM != null) {
      mockRM.stop();
    }
  }

  protected void startScheduler() throws Exception {
    RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(csConf);
    mockRM = new MockRM(csConf) {
      protected RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    cs = (CapacityScheduler) mockRM.getResourceScheduler();
    cs.updatePlacementRules();
    mockRM.start();
    cs.start();
    autoQueueHandler = cs.getCapacitySchedulerQueueManager();
    mockRM.registerNode("h1:1234", 32000, 32); // label = x
  }

  private void createMixedConfig() {
    // root
    // - a [memory=10%, vcores=1]
    //   - a1 [memory=100%, vcores=100%]
    //   - a2-auto [memory=2048, vcores=2]
    // - b [memory=1024, vcores=10%]
    // - c-auto [memory=1w, vcores=1w]
    // - d [memory=20%, vcores=20%]
    //   - d1-auto [memory=10%, vcores=1]
    // - e-auto [memory=1w, vcores=10]
    //   - e1-auto [memory=2048, vcores=2]

    // Set up static queues
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[]{"a", "b", "d"});
    csConf.setCapacityVector(A, NL, "[memory=10%, vcores=2]");
    csConf.setCapacityVector(B, NL, "[memory=2000, vcores=10%]");
    csConf.setCapacityVector(D, NL, "[memory=10%, vcores=10%]");
    csConf.setQueues(A, new String[]{"a1"});
    csConf.setCapacityVector(A1, NL, "[memory=100%, vcores=100%]");
    csConf.setAutoQueueCreationV2Enabled(CapacitySchedulerConfiguration.ROOT, true);
    csConf.setAutoQueueCreationV2Enabled(A, true);
    csConf.setAutoQueueCreationV2Enabled(D, true);

    // Set up dynamic queue templates
    csConf.set(getTemplateKey(CapacitySchedulerConfiguration.ROOT, "capacity"), "[memory=2w, vcores=5w]");
    csConf.set(getParentTemplateKey(CapacitySchedulerConfiguration.ROOT, "capacity"), "[memory=2w, vcores=10]");
    csConf.set(getLeafTemplateKey(CapacitySchedulerConfiguration.ROOT + ".*", "capacity"), "[memory=2000, vcores=2]");
    csConf.set(getLeafTemplateKey(D, "capacity"), "[memory=1000, vcores=1]");

  }
  /*
   Create and validate the following structure with mixed resource vectors and non-legacy queue mode:

                           root
      ┌─────┬────────┬─────┴─────┬─────────┐
      a     b      c-auto     e-auto       d
      |                        |           |
     a1                      e1-auto    d1-auto
    */
  @Test
  public void testAutoCreateQueueRefresh() throws Exception {
    createMixedConfig();
    startScheduler();

    createQueue("root.c-auto");

    // Check if queue c-auto got created
    CSQueue cAuto = cs.getQueue("root.c-auto");
    // At this point queues a, b, d exists, ant c-autp was just created
    // b takes 2000 MB from the cluster, a and d take up 10 + 10 = 20% (6000 MB, 6 vcore), so c-auto should get the rest
    // (24000 MB, 24 vcore) because it's the only one with configured weights
    Assert.assertEquals(24 / 32f, cAuto.getAbsoluteCapacity(), 1e-4);
    Assert.assertEquals(-1f, cAuto.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(24000,
            cAuto.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    Assert.assertEquals(24,
            cAuto.getQueueResourceQuotas().getEffectiveMinResource().getVirtualCores());
    Assert.assertEquals(((LeafQueue) cAuto).getUserLimitFactor(), -1, 1e-6);
    Assert.assertEquals(((LeafQueue) cAuto).getMaxAMResourcePerQueuePercent(), 1, 1e-6);

    // Now add another queue-d, in the same hierarchy
    createQueue("root.d.d1-auto");

    // Because queue-d has the same weight of other sibling queue, its abs cap
    // become 1/4
    CSQueue dAuto = cs.getQueue("root.d.d1-auto");
    // d1-auto should get 1000 MB, 1 vcore
    Assert.assertEquals(1 / 32f, dAuto.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(-1f, dAuto.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(1000,
            dAuto.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    Assert.assertEquals(1,
            dAuto.getQueueResourceQuotas().getEffectiveMinResource().getVirtualCores());

    createQueue("root.a.a2-auto");

    CSQueue a2Auto = cs.getQueue("root.a.a2-auto");
    Assert.assertEquals(2 / 32f, a2Auto.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(-1f, a2Auto.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(2000,
            a2Auto.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    Assert.assertEquals(2,
            a2Auto.getQueueResourceQuotas().getEffectiveMinResource().getVirtualCores());

    // Absolute requests take precedence over percentage and weight, hence a1 should have 1000 MB, 0 vcore
    CSQueue a1 = cs.getQueue("root.a.a1");
    Assert.assertEquals(1000,
            a1.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    Assert.assertEquals(0,
            a1.getQueueResourceQuotas().getEffectiveMinResource().getVirtualCores());

    createQueue("root.e-auto.e1-auto");

    // e-auto has weights configured, so it will share the remaining resources with c-auto
    CSQueue eAuto = cs.getQueue("root.e-auto");
    Assert.assertEquals(12 / 32f, eAuto.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(-1f, eAuto.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(12000,
            eAuto.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    Assert.assertEquals(10,
            eAuto.getQueueResourceQuotas().getEffectiveMinResource().getVirtualCores());

    // Now we check queue c-auto again, it should have shared its resources with e-auto
    Assert.assertEquals(12 / 32f, cAuto.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(-1f, cAuto.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(12000,
            cAuto.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    Assert.assertEquals(16,
            cAuto.getQueueResourceQuotas().getEffectiveMinResource().getVirtualCores());

    // Under e, there's only one queue, so e1 should have what it's asking for
    CSQueue e1Auto = cs.getQueue("root.e-auto.e1-auto");
    Assert.assertEquals(2 / 32f, e1Auto.getAbsoluteCapacity(), 1e-6);
    Assert.assertEquals(-1f, e1Auto.getQueueCapacities().getWeight(), 1e-6);
    Assert.assertEquals(2000,
            e1Auto.getQueueResourceQuotas().getEffectiveMinResource().getMemorySize());
    Assert.assertEquals(2,
            e1Auto.getQueueResourceQuotas().getEffectiveMinResource().getVirtualCores());
  }

  protected AbstractLeafQueue createQueue(String queuePath) throws YarnException,
          IOException {
    return autoQueueHandler.createQueue(new QueuePath(queuePath));
  }

  private String getTemplateKey(String queuePath, String entryKey) {
    return CapacitySchedulerConfiguration.getQueuePrefix(queuePath)
            + AutoCreatedQueueTemplate.AUTO_QUEUE_TEMPLATE_PREFIX + entryKey;
  }

  private String getParentTemplateKey(String queuePath, String entryKey) {
    return CapacitySchedulerConfiguration.getQueuePrefix(queuePath)
            + AutoCreatedQueueTemplate.AUTO_QUEUE_PARENT_TEMPLATE_PREFIX + entryKey;
  }

  private String getLeafTemplateKey(String queuePath, String entryKey) {
    return CapacitySchedulerConfiguration.getQueuePrefix(queuePath)
            + AutoCreatedQueueTemplate.AUTO_QUEUE_LEAF_TEMPLATE_PREFIX + entryKey;
  }
}
