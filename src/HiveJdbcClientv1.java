import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
public class HiveJdbcClientv1 {
  private static String driverName = "org.apache.hive.jdbc.HiveDriver";
  /**
   * @param args
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {
      try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.exit(1);
    }
    //replace "hive" here with the name of the user the queries should run as
    //Connection con = DriverManager.getConnection("jdbc:hive2://synazurehadoop-mn0.centralus.cloudapp.azure.com:10000/default", "hive", "hive_password");
      //Connection con = DriverManager.getConnection("jdbc:hive2://172.20.214.12:10000/default", "hive", "hive_password");
      Connection con = DriverManager.getConnection("jdbc:hive2://172.17.50.18:10000/default", "hive", "hive_password");
      
      Statement stmt = con.createStatement();
    //String tableName = "testHiveDriverTable";
    //stmt.execute("drop table if exists " + tableName);
    //stmt.execute("create table " + tableName + " (key int, value string)");
    // show tables
    // String sql = "show tables '" + tableName + "'";
    String sql = ("show tables");
    ResultSet res = stmt.executeQuery(sql);
while (res.next()) {
        System.out.println(res.getString(1));
      }
  }
}