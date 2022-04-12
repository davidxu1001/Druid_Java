import java.sql.SQLException;

public class Druid_Java_Tester {
    public static void main(String[] args) {
        // String ip = "34.150.9.50", port = "", user = "", password = "";
        String ip = "127.0.0.1", port = "", user = "", password = "";

        //BaseAdapter adapter = null;
        DruidAdapter adapter = null;
        try {
            adapter = new DruidAdapter();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        adapter.initConnect(ip, port, user, password);
        adapter.insertData("WOW_DI-4.csv");

        try {
            adapter.query1a();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }

    }
}
