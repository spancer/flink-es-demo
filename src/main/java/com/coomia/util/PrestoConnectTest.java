/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.coomia.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * ClassName:PrestoConnectTest Function: TODO ADD FUNCTION. Reason: TODO ADD REASON. Date: 2021年3月2日
 * 上午10:27:54
 *
 * @author Administrator
 * @version
 * @since JDK 1.6
 * @see
 */
public class PrestoConnectTest {

  private static Statement statement;
  private static Connection conn;

  public static Connection createConnection() throws SQLException, ClassNotFoundException {
    Class.forName("com.facebook.presto.jdbc.PrestoDriver");
    Properties properties = new Properties();
    properties.setProperty("user", "root");
    return DriverManager.getConnection("jdbc:presto://prestodb:9999/hive/default", properties);
  }

  public static void main(String[] args) throws Exception {
    String sql = "select * from t2";
    conn = createConnection();
    statement = conn.createStatement();
    ResultSet rs = statement.executeQuery(sql);
    while (rs.next()) {
      System.out.println(
          rs.getInt("id")
              + ","
              + rs.getString("name")
              + ","
              + rs.getObject("hobby")
              + ","
              + rs.getObject("add"));
    }
    statement.close();
    conn.close();
  }
}
