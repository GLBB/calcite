/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.examples;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.FrameworkConfig;

import org.apache.calcite.tools.RelBuilder;

import org.junit.jupiter.api.Test;

public class MyRelBuilderExample {

  @Test
  public void testHello() {
    System.out.println("hello");
  }

  FrameworkConfig config = RelBuilderTest.config().build();
  RelBuilder relBuilder = RelBuilder.create(config);

  @Test
  public void testRelBuilder() {

    RelNode node = relBuilder.scan("EMP")
        .build();
    System.out.println(RelOptUtil.toString(node));
  }

  @Test
  public void testProject(){
    RelNode node = relBuilder.scan("EMP")
        .project(relBuilder.field("DEPTNO"), relBuilder.field("ENAME"))
        .build();
    System.out.println(RelOptUtil.toString(node));
  }
}
