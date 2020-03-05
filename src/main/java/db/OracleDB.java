package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OracleDB {


    private static final boolean SQL_CONN = true;
    private static final boolean SQL_DEBUG = true;

    public static final String SERVER_HOSTNAME = "192.168.57.4";

    //Syncronized Singleton
    private static OracleDB instance;

    private OracleDB() {
    }

    public static OracleDB getSession() {
        if (instance == null) {
            synchronized (OracleDB.class) {
                if (instance == null) {
                    instance = new OracleDB();
                }
            }
        }
        return instance;
    }

    protected Connection conn;

    protected String jdbcDriver = "jdbc:oracle:thin:";
    protected String jdbcIP = SERVER_HOSTNAME;
    protected String jdbcPort = "1521";
    protected String jdbcCatalog = "orcl";

    protected String jdbcUser = "hr";
    protected String jdbcPassword = "hr";

    private boolean autoclose = true;

    /**
     * Getter para la clase Conexion
     *
     * @return Conexion
     */
    public Connection getConn() {
        return conn;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public String getJdbcIP() {
        return jdbcIP;
    }

    public void setJdbcIP(String jdbcIP) {
        this.jdbcIP = jdbcIP;
    }

    public String getJdbcPort() {
        return jdbcPort;
    }

    public void setJdbcPort(String jdbcPort) {
        this.jdbcPort = jdbcPort;
    }

    public String getJdbcCatalog() {
        return jdbcCatalog;
    }

    public void setJdbcCatalog(String jdbcCatalog) {
        this.jdbcCatalog = jdbcCatalog;
    }

    public String getJdbcUser() {
        return jdbcUser;
    }

    public void setJdbcUser(String jdbcUser) {
        this.jdbcUser = jdbcUser;
    }

    public String getJdbcPassword() {
        return jdbcPassword;
    }

    public void setJdbcPassword(String jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
    }

    public static void printSql(Object sql) {
        if (SQL_DEBUG) {
            System.out.println(sql.toString());
        }
    }

    /**
     * establece la conexion a la DB
     *
     * @return true si la conexion fue establecida correctamente
     */
    public boolean connect(String jdbcIP, String jdbcPort, String jdbcCatalog, String jdbcUser, String jdbcPassword) {
        boolean success = false;
        try {
            if (conn == null || conn.isClosed()) {
                String string = jdbcDriver + "@" + jdbcIP + ":" + jdbcPort + ":" + jdbcCatalog;
                System.out.println(string);
                conn = DriverManager.getConnection(string, jdbcUser, jdbcPassword);
                if (SQL_CONN) {
                    System.out.println("Connection to " + conn.getMetaData().getDriverName() + " has been established. Catalog: " + conn.getCatalog());
                }
            } else {
                if (SQL_CONN) {
                    System.out.println("Connection to " + conn.getMetaData().getDriverName() + " already active.");
                }
            }
            success = true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return success;
    }

    public boolean connect(String catalog) {
        return connect(jdbcIP, jdbcPort, catalog, jdbcUser, jdbcPassword);
    }

    public boolean connect() {
        return connect(jdbcIP, jdbcPort, jdbcCatalog, jdbcUser, jdbcPassword);
    }

    public boolean connectTry() {
        return connect(jdbcIP, jdbcPort, "", jdbcUser, jdbcPassword);
    }

    public boolean isLinkValid() {
        boolean valid = false;
        System.out.println(toString());
        if (connectTry()) {
            try {
                valid = conn.isValid(30);
            } catch (SQLException ex) {
                ex.printStackTrace();
            } finally {
                close();
            }
        }
        return valid;
    }

    public boolean isSessionValid() {
        boolean valid = false;
        if (connect()) {
            try {
                valid = conn.isValid(30);
            } catch (SQLException ex) {
                ex.printStackTrace();
            } finally {
                close();
            }
        }
        return valid;
    }

    /**
     * Finaliza una conexion a la DB
     */
    public void close() {
        try {
            if (autoclose) {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                    if (SQL_CONN) {
                        System.out.println("Connection has been closed.");
                    }
                } else {
                    if (SQL_CONN) {
                        System.out.println("Connection was already closed.");
                    }
                }
            } else {
                if (SQL_CONN) {
                    System.out.println("Keeping conection alive.");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param autoclose
     */
    public void setAutoclose(boolean autoclose, String catalog) {
        if (autoclose) {
            this.autoclose = autoclose;
            close();
        } else {
            connect(catalog);
            this.autoclose = autoclose;
        }
        if (SQL_CONN) {
            System.out.println("Autoclose? " + autoclose);
        }
    }

    public void setAutoclose(boolean autoclose) {
        setAutoclose(autoclose, jdbcCatalog);
    }

}
