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

package org.apache.calcite.test;

import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.apache.calcite.util.TestUtil;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.sql.*;
import java.util.List;
import java.util.Properties;

public class MyCsvTest {

  @Test
  void testSelectMy() throws SQLException {
    String sql = "select * from EMPS";
    Connection connection = null;
    Statement statement = null;
    try {
      Properties info = new Properties();
      String absolutePath = Sources.of(CsvTest.class.getResource("/model.json")).file().getAbsolutePath();
      info.put("model", absolutePath);
      connection = DriverManager.getConnection("jdbc:calcite:", info);
      statement = connection.createStatement();
      final ResultSet resultSet =
          statement.executeQuery(sql);
      this.output(resultSet);
    } finally {
//      close(connection, statement);
    }
  }

  @Test
  public void testFileApi() throws URISyntaxException, IOException {
    URL resource = CsvTest.class.getResource("/model.json");
    File file = new File(resource.toURI());
    List<String> lines = Files.readAllLines(file.toPath());
    System.out.println(lines);
  }

  @Test
  public void testView() throws SQLException {
    String sql = "select * from FEMALE_EMPS";
    Source viewConfigSource = Sources.of(CsvTest.class.getResource("/model-with-view.json"));
    String fileAbsPath = viewConfigSource.file().getAbsolutePath();
    Properties props = new Properties();
    props.put("model", fileAbsPath);
    Connection conn = DriverManager.getConnection("jdbc:calcite:", props);
    Statement stat = conn.createStatement();
    ResultSet resultSet = stat.executeQuery(sql);
    this.output(resultSet);
  }

  @Test
  public void testCustomTable() throws SQLException {
    String sql = "select * from EMPS";
    Source viewConfigSource = Sources.of(CsvTest.class.getResource("/model-with-custom-table.json"));
    String fileAbsPath = viewConfigSource.file().getAbsolutePath();
    Properties props = new Properties();
    props.put("model", fileAbsPath);
    Connection conn = DriverManager.getConnection("jdbc:calcite:", props);
    Statement stat = conn.createStatement();
    ResultSet resultSet = stat.executeQuery(sql);
    this.output(resultSet);
  }

  @Test
  public void testPlanRule() throws SQLException {
    String sql = "explain plan for select name from emps";
    Source modelSource = Sources.of(MyCsvTest.class.getResource("/model.json"));
    String modelPath = modelSource.file().getAbsolutePath();
    Source smartSource = Sources.of(MyCsvTest.class.getResource("/smart.json"));
    String smartPath = smartSource.file().getAbsolutePath();
    // model
    Properties props = new Properties();
    props.put("model", modelPath);
    Connection conn = DriverManager.getConnection("jdbc:calcite:", props);
    Statement stat = conn.createStatement();
    ResultSet resultSet = stat.executeQuery(sql);
    this.output(resultSet);
    // smart
    Properties props2 = new Properties();
    props.put("model", smartPath);
    Connection conn2 = DriverManager.getConnection("jdbc:calcite:", props);
    Statement stat2 = conn2.createStatement();
    ResultSet resultSet2 = stat2.executeQuery(sql);
    this.output(resultSet2);


  }

  private Void output(ResultSet resultSet) {
    try {
      output(resultSet, System.out);
    } catch (SQLException e) {
      throw TestUtil.rethrow(e);
    }
    return null;
  }

  private void output(ResultSet resultSet, PrintStream out)
      throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    while (resultSet.next()) {
      for (int i = 1;; i++) {
        out.print(resultSet.getString(i));
        if (i < columnCount) {
          out.print(", ");
        } else {
          out.println();
          break;
        }
      }
    }
  }



}
