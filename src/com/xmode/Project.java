package com.xmode;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Calendar;

public class Project {

    /**
     * There's a few ways to do this, but I am going to do this kind of quick and dirty.
     * Ideally, this would be clean with a few convenience methods which allows us to get
     * the data from the models/domains, grab the SQL connection info, grab the correct SQL
     * statements, dates, etc. For now, we'll just manually do this as more or less
     * pseudo-code ( though it works ).
     */

    public static void main(String[] args) {

        try {
            ResultSet rs = getResultSet();
            convertResultToCSV(rs);
        } catch (SQLException | FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    private static ResultSet getResultSet() throws SQLException {
        Connection connection = getConnection();
        String theStatement = "SELECT * from location WHERE timestamp >= ? AND timestamp < DATE_ADD(?, INTERVAL 1 DAY);";
        Date date = getDate();

        ResultSet rs = null;
        try {
            PreparedStatement selectDate = connection.prepareStatement(theStatement, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            selectDate.setFetchSize(Integer.MIN_VALUE);
            selectDate.setString(1, String.valueOf(date));
            selectDate.setString(2, String.valueOf(date));
            rs = selectDate.executeQuery();
        } catch (SQLException e) {
            System.out.println("Error code: " + e.getErrorCode() + "\n");
            e.printStackTrace();
        }

        return rs;
    }

    private static Connection getConnection() throws SQLException {

        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "rootbeer"); //dummy

        System.out.println("We be connected.");
        return conn;
    }

    // Lets assume we actually get our dates from someplace nice, like a domain/model object.
    // For now, lets get it this dirty way.
    private static Date getDate() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 2017);
        cal.set(Calendar.MONTH, Calendar.MAY);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        java.util.Date utilDate = new java.util.Date(cal.getTimeInMillis());

        return new java.sql.Date(utilDate.getTime());
    }

    private static void convertResultToCSV(ResultSet rs) throws SQLException, FileNotFoundException {
        // I'm on a Windows machine, sadly, so here's to my tmp directory.
        // I believe PrintWriter uses BufferWriter so it innately should buffer this output.
        PrintWriter writer = new PrintWriter(new File("e:/tmp/data.csv"));
        ResultSetMetaData meta = rs.getMetaData();
        int columns = meta.getColumnCount();
        String headers = "\"" + meta.getColumnName(1) + "\"";
        for (int i = 2; i < columns + 1; i++) {
            headers += ",\"" + meta.getColumnName(i).replaceAll("\"", "\\\"") + "\"";
        }
        writer.println(headers);
        while (rs.next()) {
            String row = "\"" + rs.getString(1).replaceAll("\"", "\\\"") + "\"";
            for (int i = 2; i < columns + 1; i++) {
                row += ",\"" + rs.getString(i).replaceAll("\"", "\\\"") + "\"";
            }
            writer.println(row);
        }
        writer.close();
    }
}
