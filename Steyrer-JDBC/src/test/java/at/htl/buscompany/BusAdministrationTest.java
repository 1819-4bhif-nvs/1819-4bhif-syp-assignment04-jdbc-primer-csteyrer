package at.htl.buscompany;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class BusAdministrationTest {


    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    public static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db";
    public static final String USER = "app";
    public static final String PASSWORD = "app";
    public static Connection conn;

    @BeforeClass
    public static void initJDBC(){
        try {
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Verbindung zur Datenbank nicht möglich\n" + e.getMessage() + "\n");
            System.exit(1);
        }

        try {
            Statement stmt = conn.createStatement();
            String sql = "CREATE TABLE bus (" +
                    "id INT CONSTRAINT bus_pk PRIMARY KEY " +
                    "GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)," +
                    "driver VARCHAR(30) NOT NULL," +
                    "bus_type VARCHAR(20) NOT NULL" +
                    ")";
            stmt.execute(sql);
            sql = "INSERT INTO bus (driver, bus_type) values('Driver1', 'Gelenkbus')";
            stmt.execute(sql);
            sql = "INSERT INTO bus (driver, bus_type) values('Driver2', 'Doppelgelenkbus')";
            stmt.execute(sql);
            System.out.println("Tabelle bus erstellt!");

            sql = "CREATE TABLE bus_stop (" +
                    "id INT CONSTRAINT bus_stop_pk PRIMARY KEY " +
                    "GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)," +
                    "bus_stop_name VARCHAR(30) NOT NULL" +
                    ")";
            stmt.execute(sql);
            sql = "INSERT INTO bus_stop (bus_stop_name) values('Stop1')";
            stmt.execute(sql);
            sql = "INSERT INTO bus_stop (bus_stop_name) values('Stop2')";
            stmt.execute(sql);
            System.out.println("Tabelle bus_stop erstellt!");

            sql = "CREATE TABLE schedule (" +
                    "id INT CONSTRAINT schedule_pk PRIMARY KEY " +
                    "GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)," +
                    "bus_id INT CONSTRAINT bus_fk references bus(id)," +
                    "bus_stop_id INT CONSTRAINT bus_stop_fk references bus_stop(id)," +
                    "stop_time TIMESTAMP NOT NULL" +
                    ")";
            stmt.execute(sql);
            String dateFormat = "YYYY-MM-DD HH24:MI";
            sql = "INSERT INTO schedule (bus_id, bus_stop_id, stop_time) values(1, 2, " +
                    "TIMESTAMP('2018-10-20', '06:00:00'))";
            stmt.execute(sql);
            sql = "INSERT INTO schedule (bus_id, bus_stop_id, stop_time) values(2, 1, " +
                    "TIMESTAMP('2018-10-20', '08:00:00'))";
            stmt.execute(sql);
            sql = "INSERT INTO schedule (bus_id, bus_stop_id, stop_time) values(2, 2, " +
                    "TIMESTAMP('2018-10-20', '08:30:00'))";
            stmt.execute(sql);
            System.out.println("Tabelle schedule erstellt!");

            System.out.println("Tabellen erstellt!\n");

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    @AfterClass
    public static void teardownJDBC(){

        try{
            conn.createStatement().execute("DROP TABLE schedule");
            System.out.println("Tabelle schedule gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle schedule konnte nicht gelöscht werden:\n"
                    + e.getMessage());
        }

        try{
            conn.createStatement().execute("DROP TABLE bus");
            System.out.println("Tabelle bus gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle bus konnte nicht gelöscht werden:\n"
                    + e.getMessage());
        }

        try{
            conn.createStatement().execute("DROP TABLE bus_stop");
            System.out.println("Tabelle bus_stop gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle bus_stop konnte nicht gelöscht werden:\n"
                    + e.getMessage());
        }


        try {
            if(conn != null && !conn.isClosed()){
                conn.close();
                System.out.println("Good bye");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBus()
    {
        try {
            PreparedStatement pstmt = conn.prepareStatement("SELECT id, driver, bus_type FROM bus order by id");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("driver"), is("Driver1"));
            assertThat(rs.getString("bus_type"), is("Gelenkbus"));

            rs.next();
            assertThat(rs.getString("driver"), is("Driver2"));
            assertThat(rs.getString("bus_type"), is("Doppelgelenkbus"));
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    @Test
    public void testBusStop()
    {
        try {
            PreparedStatement pstmt = conn.prepareStatement("SELECT id, bus_stop_name FROM bus_stop order by id");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("bus_stop_name"), is("Stop1"));

            rs.next();
            assertThat(rs.getString("bus_stop_name"), is("Stop2"));
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    @Test
    public void testSchedule()
    {
        try {
            PreparedStatement pstmt = conn.prepareStatement("SELECT id, bus_id, bus_stop_id, stop_time FROM schedule order by id, bus_id, bus_stop_id");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getTimestamp("stop_time"), is(Timestamp.valueOf("2018-10-20 06:00:00")));

            rs.next();
            assertThat(rs.getTimestamp("stop_time"), is(Timestamp.valueOf("2018-10-20 08:00:00")));

            rs.next();
            assertThat(rs.getTimestamp("stop_time"), is(Timestamp.valueOf("2018-10-20 08:30:00")));
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    @Test
    public void testMetaDataSchedule() {
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            String tableNamePattern = "SCHEDULE";
            String columnNamePattern = null;

            ResultSet rs = databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

            rs.next();
            String columnName = rs.getString(4);
            int columnType = rs.getInt(5);
            assertThat(columnName, is("ID"));
            assertThat(columnType, is(Types.INTEGER));

            rs.next();
            columnName = rs.getString(4);
            columnType = rs.getInt(5);
            assertThat(columnName, is("BUS_ID"));
            assertThat(columnType, is(Types.INTEGER));

            rs.next();
            columnName = rs.getString(4);
            columnType = rs.getInt(5);
            assertThat(columnName, is("BUS_STOP_ID"));
            assertThat(columnType, is(Types.INTEGER));


            String schema = null;
            String tableName = "SCHEDULE";

            rs = databaseMetaData.getPrimaryKeys(catalog, schema, tableName);

            rs.next();
            columnName = rs.getString(4);
            assertThat(columnName, is("ID"));


            //Foreign Key
            rs = databaseMetaData.getImportedKeys(catalog, schema, tableName);

            rs.next();
            String pkTableName = rs.getString(3);
            String pkTableColumnName = rs.getString(4);
            String fkTableName = rs.getString(7);
            String fkTableColumnName = rs.getString(8);
            assertThat(pkTableName, is("BUS"));
            assertThat(pkTableColumnName, is("ID"));
            assertThat(fkTableName, is("SCHEDULE"));
            assertThat(fkTableColumnName, is("BUS_ID"));

            rs.next();
            pkTableName = rs.getString(3);
            pkTableColumnName = rs.getString(4);
            fkTableName = rs.getString(7);
            fkTableColumnName = rs.getString(8);
            assertThat(pkTableName, is("BUS_STOP"));
            assertThat(pkTableColumnName, is("ID"));
            assertThat(fkTableName, is("SCHEDULE"));
            assertThat(fkTableColumnName, is("BUS_STOP_ID"));


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMetaDataBusStop() {
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            String tableNamePattern = "BUS_STOP";
            String columnNamePattern = null;

            ResultSet rs = databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

            rs.next();
            String columnName = rs.getString(4);
            int columnType = rs.getInt(5);
            assertThat(columnName, is("ID"));
            assertThat(columnType, is(Types.INTEGER));

            rs.next();
            columnName = rs.getString(4);
            columnType = rs.getInt(5);
            assertThat(columnName, is("BUS_STOP_NAME"));
            assertThat(columnType, is(Types.VARCHAR));


            String schema = null;
            String tableName = "BUS_STOP";

            rs = databaseMetaData.getPrimaryKeys(catalog, schema, tableName);

            rs.next();
            columnName = rs.getString(4);
            assertThat(columnName, is("ID"));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMetaDataBus() {
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            String tableNamePattern = "BUS";
            String columnNamePattern = null;

            ResultSet rs = databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);

            rs.next();
            String columnName = rs.getString(4);
            int columnType = rs.getInt(5);
            assertThat(columnName, is("ID"));
            assertThat(columnType, is(Types.INTEGER));

            rs.next();
            columnName = rs.getString(4);
            columnType = rs.getInt(5);
            assertThat(columnName, is("DRIVER"));
            assertThat(columnType, is(Types.VARCHAR));

            rs.next();
            columnName = rs.getString(4);
            columnType = rs.getInt(5);
            assertThat(columnName, is("BUS_TYPE"));
            assertThat(columnType, is(Types.VARCHAR));


            String schema = null;
            String tableName = "BUS";

            rs = databaseMetaData.getPrimaryKeys(catalog, schema, tableName);

            rs.next();
            columnName = rs.getString(4);
            assertThat(columnName, is("ID"));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
