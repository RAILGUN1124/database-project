//Responsible for connecting to database server
//References//
//ChatGBT//
//Stack overflow//
//piazza//
import java.sql.SQLException;
import java.util.Properties;
import oracle.jdbc.pool.OracleDataSource;
import oracle.jdbc.OracleConnection;
public class Server {
    final static String DB_URL = "jdbc:oracle:thin:@cs174a_tp?TNS_ADMIN=/Users/???/Downloads/Wallet_cs174aa";
    final static String DB_USER = "ADMIN";
    final static String DB_PASSWORD = "???";
    OracleConnection connection;
    public Server() throws SQLException{
        Properties info = new Properties();
        System.out.println("Initializing connection properties...");
        info.put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, DB_USER);
        info.put(OracleConnection.CONNECTION_PROPERTY_PASSWORD, DB_PASSWORD);
        info.put(OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH, "20");
        System.out.println("Creating OracleDataSource...");
        OracleDataSource ods = new OracleDataSource();
        System.out.println("Setting connection properties...");
        ods.setURL(DB_URL);
        ods.setConnectionProperties(info);
        try{
            connection = (OracleConnection) ods.getConnection();
        }
        catch (Exception ignored) {
            System.out.println("CONNECTION ERROR:");
            System.out.println(ignored);
        }
    }
    public void exit() {
        try {
            connection.close();
        } catch (SQLException ignored) {
            ignored.printStackTrace();
        }
    }
}
