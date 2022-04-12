import java.sql.SQLException;

public class Druid_Tester {
    public static void main(String[] args) {
        // String ip = "34.150.9.50", port = "", user = "", password = "";
        String ip = "127.0.0.1", port = "", user = "", password = "";
        int numLoops = 10;

        //BaseAdapter adapter = null;
        DruidAdapter adapter = null;
        try {
            adapter = new DruidAdapter();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        adapter.initConnect(ip, port, user, password);
				//DruidAdapter.appendToFile("Druid DB Query.csv", importData(adapter, "EGL_DI-2.csv")); 
				DruidAdapter.appendToFile("Druid DB Query.csv", importData(adapter, "WOW_DI-4.csv")); 

    }

    private static String importData(DruidAdapter adapter, String filename) {
        double time = adapter.insertData(filename);
        return "Time spent for receiving respond (not ingestion) is " + time + "\n";
    }

    private static Double query1a(DruidAdapter adapter) {
        try {
            return adapter.query1a();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return 0.0;
    }

    private static Double query1b(DruidAdapter adapter) {
        try {
            return adapter.query1b();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return 0.0;
    }

    private static Double query1c(DruidAdapter adapter) {
        try {
            return adapter.query1c();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return 0.0;
    }

    private static Double query2(DruidAdapter adapter) {
        try {
            return adapter.query2();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return 0.0;
    }

    private static Double query3(DruidAdapter adapter) {
        try {
            return adapter.query3();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return 0.0;
    }
}
