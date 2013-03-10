/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.exec.ref;

import static org.junit.Assert.*;

import java.util.Collection;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.rse.RSERegistry;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class RunSimplePlan{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RunSimplePlan.class);
  
  
  @Test
  public void parseSimplePlan() throws Exception{

    DrillConfig config = DrillConfig.create();
    LogicalPlan plan = LogicalPlan.parse(config, Files.toString(FileUtils.getResourceAsFile("/simple_plan.json"), Charsets.UTF_8));
      logger.error("=============================0");
    IteratorRegistry ir = new IteratorRegistry();
      logger.error("=============================1");
    ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
      logger.error("=============================2");
    i.setup();
      logger.error("=============================3");
    Collection<RunOutcome> outcomes = i.run();
      logger.error("=============================4");
    //assertEquals(outcomes.size(), 1);
    //assertEquals(outcomes.iterator().next().records, 2);

  }

    @Test
    public void parseSimpleScan() throws Exception{

        DrillConfig config = DrillConfig.create();
        LogicalPlan plan = LogicalPlan.parse(config, Files.toString(FileUtils.getResourceAsFile("/simple_scan.json"), Charsets.UTF_8));
        logger.error("=============================0");
        IteratorRegistry ir = new IteratorRegistry();
        logger.error("=============================1");
        ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
        logger.error("=============================2");
        i.setup();
        logger.error("=============================3");
        Collection<RunOutcome> outcomes = i.run();
        logger.error("=============================4");
        //assertEquals(outcomes.size(), 1);
        //assertEquals(outcomes.iterator().next().records, 2);

    }

    @Test
    public void parseSimpleFilter() throws Exception{

        DrillConfig config = DrillConfig.create();
        LogicalPlan plan = LogicalPlan.parse(config, Files.toString(FileUtils.getResourceAsFile("/simple_filter.json"), Charsets.UTF_8));
        logger.error("=============================0");
        IteratorRegistry ir = new IteratorRegistry();
        logger.error("=============================1");
        ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
        logger.error("=============================2");
        i.setup();
        logger.error("=============================3");
        Collection<RunOutcome> outcomes = i.run();
        logger.error("=============================4");
        //assertEquals(outcomes.size(), 1);
        //assertEquals(outcomes.iterator().next().records, 2);

    }

    @Test
    public void parseSimpleAggregate() throws Exception{

        DrillConfig config = DrillConfig.create();
        LogicalPlan plan = LogicalPlan.parse(config, Files.toString(FileUtils.getResourceAsFile("/simple_collapsingaggregate.json"), Charsets.UTF_8));
        logger.error("=============================0");
        IteratorRegistry ir = new IteratorRegistry();
        logger.error("=============================1");
        ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
        logger.error("=============================2");
        i.setup();
        logger.error("=============================3");
        Collection<RunOutcome> outcomes = i.run();
        logger.error("=============================4");
        //assertEquals(outcomes.size(), 1);
        //assertEquals(outcomes.iterator().next().records, 2);

    }

    @Test
    public void parseSimpleProject() throws Exception{

        DrillConfig config = DrillConfig.create();
        LogicalPlan plan = LogicalPlan.parse(config, Files.toString(FileUtils.getResourceAsFile("/simple_project.json"), Charsets.UTF_8));
        logger.error("=============================0");
        IteratorRegistry ir = new IteratorRegistry();
        logger.error("=============================1");
        ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
        logger.error("=============================2");
        i.setup();
        logger.error("=============================3");
        Collection<RunOutcome> outcomes = i.run();
        logger.error("=============================4");
        //assertEquals(outcomes.size(), 1);
        //assertEquals(outcomes.iterator().next().records, 2);

    }

    @Test
    public void parseSimpleJoinPlan() throws Exception{

        DrillConfig config = DrillConfig.create();
        LogicalPlan plan = LogicalPlan.parse(config, Files.toString(FileUtils.getResourceAsFile("/simple_join.json"), Charsets.UTF_8));
        logger.error("=============================0");
        IteratorRegistry ir = new IteratorRegistry();
        logger.error("=============================1");
        ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
        logger.error("=============================2");
        i.setup();
        logger.error("=============================3");
        Collection<RunOutcome> outcomes = i.run();
        logger.error("=============================4");
        //assertEquals(outcomes.size(), 1);
        //assertEquals(outcomes.iterator().next().records, 2);

    }

    @Test
    public void parseSimpleSelectPlan() throws Exception{

        DrillConfig config = DrillConfig.create();
        LogicalPlan plan = LogicalPlan.parse(config, Files.toString(FileUtils.getResourceAsFile("/simple_select.json"), Charsets.UTF_8));
        logger.error("=============================0");
        IteratorRegistry ir = new IteratorRegistry();
        logger.error("=============================1");
        ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
        logger.error("=============================2");
        i.setup();
        logger.error("=============================3");
        Collection<RunOutcome> outcomes = i.run();
        logger.error("=============================4");
        //assertEquals(outcomes.size(), 1);
        //assertEquals(outcomes.iterator().next().records, 2);

    }
}
