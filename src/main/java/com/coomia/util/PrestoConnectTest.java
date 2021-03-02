
/**
 * Project Name:flink-es-demo File Name:PrestoConnectTest.java Package Name:com.coomia.util
 * Date:2021年3月2日上午10:27:54 Copyright (c) 2021, spancer.ray All Rights Reserved.
 *
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
    String sql = "select * from elasticsearch.";
    conn = createConnection();
    statement = conn.createStatement();
    ResultSet rs = statement.executeQuery(sql);
    while (rs.next()) {
      System.out.println(rs.getInt("id") + "," + rs.getString("name") + "," + rs.getObject("hobby")
          + "," + rs.getObject("add"));
    }
    statement.close();
    conn.close();
  }


}

