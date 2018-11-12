package ru.milandr.courses.khaidukov;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

public class Application {

    public static int numOfRows (ResultSet rs) throws SQLException {
        int num = 0;

        try{
            rs.last();
            num = rs.getRow();
            rs.beforeFirst();
        } catch (SQLException e) { throw e; }

        return num;
    }

    public static List<Map<String, String>> getDataSet(String url, String usr, String psd, String tbl, String[] cols) throws SQLException {
        Connection conn = null;

        try {
            Class.forName("org.postgresql.Driver");

            conn = DriverManager.getConnection(url, usr, psd);
            if (conn == null) {
                System.out.println("No DB connection");
                System.exit(0);
            }

            Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl);

            List<Map<String, String>> dataSet = new ArrayList<Map<String, String>>();

            IntStream stream1 = IntStream.range(0, numOfRows(rs));
            stream1.forEach(i -> {
                try {
                    if (rs.next()) {
                        Map rec = new HashMap<String, String>();

                        IntStream stream2 = IntStream.range(0, cols.length);
                        stream2.forEach(j -> {
                            try {
                                rec.put(cols[j], rs.getString(cols[j]));
                            }catch (SQLException e) {
                                e.printStackTrace();
                                System.exit(-1);
                            }
                        });
                        dataSet.add(rec);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            });

            return dataSet;

        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
        return null;
    }

    public static boolean isSame (List<Map<String, String>> ds1, List<Map<String, String>> ds2){
        if (ds1.size() != ds2.size()) return false;

        AtomicBoolean flag = new AtomicBoolean(true);

        ds1.forEach(i -> {
            if (!ds2.remove(i)){ flag.set(false); }
        });

        return flag.get();
    }

    public static void main(String[] args) throws SQLException {
        Scanner in = new Scanner(System.in);

        System.out.println("Data Base 1\nURL:");
        String url1 = in.nextLine();
        System.out.println("User:");
        String usr1 = in.nextLine();
        System.out.println("Password:");
        String psd1 = in.nextLine();

        System.out.println("\nData Base 2\nURL:");
        String url2 = in.nextLine();
        System.out.println("User:");
        String usr2 = in.nextLine();
        System.out.println("Password:");
        String psd2 = in.nextLine();

        System.out.println("\nDB1 Table name:");
        String tbl1 = in.nextLine();

        System.out.println("DB2 Table name:");
        String tbl2 = in.nextLine();

        System.out.println("Column names:");
        String[] columns = in.nextLine().split(",");

        List<Map<String, String>> ds1 = getDataSet(url1, usr1, psd1, tbl1, columns);
        List<Map<String, String>> ds2 = getDataSet(url2, usr2, psd2, tbl2, columns);

        System.out.println(isSame(ds1, ds2));
    }
}