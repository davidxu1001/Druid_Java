import java.util.*;
import java.util.concurrent.TimeUnit;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import okhttp3.*;


// TODO:
// 1. Customize table names
// *. port, user, password is not used

//public class DruidAdapter implements BaseAdapter {
public class DruidAdapter {

    private final String dataSourceName = ""; 
    private final String baseTableName = "base_table"; // table storing financial instrument information
    private final String splitTableName = "split_table"; // table storing split events

    private String writeURL = ":8081/druid/indexer/v1/task";
    private String queryURL = ":8082/druid/v2/sql/avatica-protobuf/;serialization=protobuf";
    private static final Integer TIMEOUT = 500000; // default, used for client builder timeouts
    private final MediaType MEDIA_TYPE_TEXT = MediaType.parse("application/json");

    //// helper functions ////
    private static final OkHttpClient OK_HTTP_CLIENT = new OkHttpClient().newBuilder()
            .readTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
            .connectTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
            .writeTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
            .build();

    public static OkHttpClient getOkHttpClient() {
        return OK_HTTP_CLIENT;
    }

    // execute a already-built, http request from an okHttpClient
    // output the response code and return the time elapsed
    private double exeOkHttpRequest(Request request) {
        double costTime = 0L;
        Response response;
        OkHttpClient client = getOkHttpClient();
        try {
            double startTime = System.nanoTime();
            System.out.println("sending a request");
            response = client.newCall(request).execute();
            System.out.println("received a response:");
            int code = response.code();
            System.out.println("code " + code + ", " + response.body().string() + "\n");
            response.close();
            double endTime = System.nanoTime();
            costTime = endTime - startTime;
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
        return costTime / 1000 / 1000;
    }

    public static void appendToFile(String filename, String msg) {
        File tmp = new File(filename);
        try {
            tmp.createNewFile();
            FileOutputStream oFile = new FileOutputStream(tmp, true);
            oFile.write(msg.getBytes());
            oFile.flush();
			oFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return;
    }

    //// Adapter functions ////
    public void initConnect(String ip, String port, String user, String password) {
        writeURL = "http://" + ip + writeURL;
        queryURL = "jdbc:avatica:remote:url=" + "http://" + ip + queryURL;
    }

	public double insertData(String filename) {
		double costTime = 0L;
		String json =
			"{ " +
			"	\"type\": \"index\", " +
			"	\"spec\": { " +
			"		\"ioConfig\": { " +
			"			\"type\": \"index\", " +
			"			\"inputSource\": { " +
			"				\"type\": \"local\", " +
			"				\"baseDir\": \"/Users/111420/Documents/DruidDB_Tester_large_parquet\", " +
			"				\"filter\": \"" + filename + "\" " +
			"			}, " +
			"			\"inputFormat\": {" +
			"				\"type\": \"csv\", " +
			"				\"findColumnsFromHeader\": true " +
			"			} " +
			"		}, " +
			"		\"tuningConfig\": { " +
			"			\"type\": \"index\", " +
			"			\"partitionsSpec\": { " +
			"				\"type\": \"dynamic\" " +
			"			} " +
			"		}, " +
			"		\"dataSchema\": { " +
			"			\"dataSource\": \"" + filename + "\", " +
			"			\"timestampSpec\": { " +
			"				\"column\": \"myTimestamp2\", " +
			"				\"format\": \"posix\" " +
			"			}, " +
			"			\"dimensionsSpec\": { " +
			"				\"dimensions\": [ " +
			"					\"batch_id\", " +
			"				{ " +
			"					\"type\": \"long\", " +
			"					\"name\": \"row_no\" " +
			"				}, " +
			"				\"ms_id\", " +
			"				\"ms_version\", " +
			"				\"pu_version\", " +
			"				{ " +
			"					\"type\": \"long\", " +
			"					\"name\": \"primary_key\" " +
			"				}, " +
			"				{ " +
			"					\"type\": \"long\", " +
			"					\"name\": \"rewards_number\" " +
			"				}, " +
			"				\"similarity_config\", " +
			"				\"edit_item_000\", " +
			"				\"edit_item_001\", " +
			"				\"edit_item_002\", " +
			"				\"edit_item_003\", " +
			"				\"edit_item_004\", " +
			"				\"edit_item_005\", " +
			"				\"edit_item_006\", " +
			"				\"edit_item_007\", " +
			"				\"edit_item_008\", " +
			"				\"edit_item_009\", " +
			"				\"edit_item_010\", " +
			"				\"edit_item_013\", " +
			"				\"edit_item_014\", " +
			"				\"edit_item_100\", " +
			"				\"edit_item_200\", " +
			"				\"edit_item_201\", " +
			"				\"edit_item_400\", " +
			"				\"edit_item_401\", " +
			"				\"edit_item_402\", " +
			"				\"edit_item_403\", " +
			"				\"edit_item_404\", " +
			"				\"edit_item_405\", " +
			"				\"edit_item_413\", " +
			"				\"edit_item_500\", " +
			"				\"edit_item_501\", " +
			"				\"edit_item_502\", " +
			"				\"edit_item_503\", " +
			"				\"edit_item_504\", " +
			"				\"edit_item_505\", " +
			"				\"edit_item_513\", " +
			"				\"edit_item_600\", " +
			"				\"edit_item_601\", " +
			"				\"edit_item_613\", " +
			"				\"edit_item_700\", " +
			"				\"edit_item_713\", " +
			"				\"edit_item_800\", " +
			"				\"edit_item_801\", " +
			"				\"edit_item_802\", " +
			"				\"edit_item_803\", " +
			"				\"edit_item_804\", " +
			"				\"edit_item_805\", " +
			"				\"edit_item_807\", " +
			"				\"edit_item_808\", " +
			"				\"edit_item_809\", " +
			"				\"edit_item_810\", " +
			"				\"edit_item_812\", " +
			"				\"edit_item_813\", " +
			"				\"edit_item_1004\", " +
			"				\"edit_item_1006\", " +
			"				\"edit_item_1010\", " +
			"				\"edit_item_1013\" " +
			"					] " +
			"			}, " +
			"			\"granularitySpec\": { " +
			"				\"queryGranularity\": \"none\", " +
			"				\"rollup\": false, " +
			"				\"segmentGranularity\": \"hour\" " +
			"			} " +
			"		} " +
			"	} " +
			"} ";

		System.out.println("\n" + json + "\n");
		// build a POST request
		Request request = null;
		try {
			request = new Request.Builder()
				.url(writeURL)
				.post(RequestBody.create(MEDIA_TYPE_TEXT, json.getBytes("UTF-8")))
				.build();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Unexpected error in sending request!");
		}

		// execute the request
		costTime = exeOkHttpRequest(request);
		return costTime;
	}

    public double query1a() throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.calcite.avatica.remote.Driver");
        Properties properties = new Properties();
        String query = 
					"SELECT Id, EXTRACT(YEAR FROM __time), " +
					"AVG(\"ClosePrice\"), MAX(\"ClosePrice\"), MIN(\"ClosePrice\") " + 
					"FROM " + dataSourceName  + 
					" WHERE EXTRACT(YEAR FROM __time) >= 2022 AND EXTRACT(YEAR FROM __time) < 2032 " +
					" GROUP BY Id, EXTRACT(YEAR FROM __time) " +
					" ORDER BY Id, EXTRACT(YEAR FROM __time) ";

        double startTime, endTime;
        try (Connection connection = DriverManager.getConnection(queryURL, properties)) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                startTime = System.nanoTime();
                final ResultSet resultSet = preparedStatement.executeQuery();
                endTime = System.nanoTime();
                resultSet.close();
            }
        }
        double accTime = endTime - startTime;
        return accTime / 1000 / 1000; // in ms
    }

    public double query1b() throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.calcite.avatica.remote.Driver");
        Properties properties = new Properties();
				String query =  
					"SELECT Id, EXTRACT(YEAR FROM __time), EXTRACT(MONTH FROM __time), " +
					"AVG(\"ClosePrice\"), MAX(\"ClosePrice\"), MIN(\"ClosePrice\") " +
					"FROM " + dataSourceName +
					" WHERE EXTRACT(YEAR FROM __time) >= 2022 AND EXTRACT(YEAR FROM __time) < 2032 " +
					"GROUP BY Id, EXTRACT(YEAR FROM __time), EXTRACT(MONTH FROM __time) " +
					"ORDER BY Id, EXTRACT(YEAR FROM __time), EXTRACT(MONTH FROM __time) ";

        double startTime, endTime;
        try (Connection connection = DriverManager.getConnection(queryURL, properties)) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                startTime = System.nanoTime();
                final ResultSet resultSet = preparedStatement.executeQuery();
                endTime = System.nanoTime();
                resultSet.close();
            }
        }
        double accTime = endTime - startTime;
        return accTime / 1000 / 1000; // in ms
    }

    public double query1c() throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.calcite.avatica.remote.Driver");
        Properties properties = new Properties();
        String query = 
					" SELECT Id, EXTRACT(YEAR FROM __time), EXTRACT(MONTH FROM __time), EXTRACT(DAY FROM __time), " +
					" AVG(\"ClosePrice\"), MAX(\"ClosePrice\"), MIN(\"ClosePrice\") " +
					" FROM " + dataSourceName +
					" WHERE EXTRACT(YEAR FROM __time) >= 2022 AND EXTRACT(YEAR FROM __time) < 2032 " +
					" GROUP BY Id, EXTRACT(YEAR FROM __time), EXTRACT(MONTH FROM __time), EXTRACT(DAY FROM __time) " + 
					" ORDER BY Id, EXTRACT(YEAR FROM __time), EXTRACT(MONTH FROM __time), EXTRACT(DAY FROM __time) ";

        double startTime, endTime;
        try (Connection connection = DriverManager.getConnection(queryURL, properties)) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                startTime = System.nanoTime();
                final ResultSet resultSet = preparedStatement.executeQuery();
                endTime = System.nanoTime();
                resultSet.close();
            }
        }
        double accTime = endTime - startTime;
        return accTime / 1000 / 1000; // in ms
    }

    public double query2() throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.calcite.avatica.remote.Driver");
        Properties properties = new Properties();
        String query = 
					" SELECT a.Id, a.__time, LowPrice, HighPrice " +
					" FROM " + dataSourceName + " a, " + splitTableName + " b " +
					" WHERE a.Id=b.Id AND a.__time=b.__time ";

        double startTime, endTime;

        try (Connection connection = DriverManager.getConnection(queryURL, properties)) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                startTime = System.nanoTime();
                final ResultSet resultSet = preparedStatement.executeQuery();
                endTime = System.nanoTime();
                resultSet.close();
            }
        }

        return (endTime - startTime) / 1000 / 1000;
    }

    public double query3() throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.calcite.avatica.remote.Driver");
        Properties properties = new Properties();
        String query = 
					" SELECT AVG(\"ClosePrice\") FROM " + dataSourceName + " a, " + baseTableName + " b " +
					" WHERE a.Id = b.Id AND b.SIC='COMPUTERS' ";

        double startTime, endTime;

        try (Connection connection = DriverManager.getConnection(queryURL, properties)) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                startTime = System.nanoTime();
                final ResultSet resultSet = preparedStatement.executeQuery();
                endTime = System.nanoTime();
                resultSet.close();
            }
        }

        return (endTime - startTime) / 1000 / 1000;
    }

}
