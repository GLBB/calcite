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

package cn.gl;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;

import org.apache.calcite.schema.SchemaPlus;

import org.apache.commons.dbcp2.BasicDataSource;

import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Properties;

public class OverviewTest {

  /**
   * 测试连接mysql 执行sql
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  @Test
  public void testConnectMysql() throws SQLException, ClassNotFoundException {
    // get root schema
    Class.forName("org.apache.calcite.jdbc.Driver");
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    info.put("quoting", "DOUBLE_QUOTE");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    Class.forName("com.mysql.jdbc.Driver");
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:mysql://localhost:3306/calcite_demo");
    dataSource.setUsername("gl");
    dataSource.setPassword("123");
    JdbcSchema schema = JdbcSchema.create(rootSchema, "hr", dataSource, null, "calcite_demo");
    rootSchema.add("hr", schema);
    calciteConnection.setSchema("hr");

    String sql = "select d.name as deptName, e.name as empName\n" +
        "from department as d\n" +
        "left join employee as e\n" +
        "on d.id = e.dept_no";
    Statement statement = calciteConnection.createStatement();
    ResultSet resultSet = statement.executeQuery(sql);
    while (resultSet.next()) {
      String deptName = resultSet.getString("deptName");
      String empName = resultSet.getString("empName");
      System.out.printf("deptName: %s, empName: %s%n", deptName, empName);
    }

    resultSet.close();
    statement.close();
    dataSource.close();
    calciteConnection.close();
  }
}
