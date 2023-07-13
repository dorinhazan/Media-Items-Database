import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class Assignment{

    private final String connectionUrl;
    // Constructor that sets the connection URL based on the provided username and password
    public Assignment(String user_name, String password) {
        this.connectionUrl = "jdbc:sqlserver://132.72.64.124:1433;databaseName=" + user_name + ";user=" + user_name + ";" + "password=" + password + ";encrypt=false;";

    }

    // Method that reads data from a file and inserts it into a database table
    public void fileToDataBase(String filePath){
        Connection con = null;
        BufferedReader br = null;
        PreparedStatement preparedStmt = null;

        try {
            // Define connection to SQL SERVER:
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            con = DriverManager.getConnection(this.connectionUrl);

            // Prepare the SQL statement that will insert the data into the database
            String query = "INSERT INTO MediaItems (MID,PROD_YEAR,TITLE) VALUES (?,?,?)";
            preparedStmt = con.prepareStatement(query);
            br = new BufferedReader(new FileReader(filePath));
            int MID = 0;
            String line = null;
            // Open the file and read the data line by line and insert it into the table
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                preparedStmt.setInt(1, MID);
                preparedStmt.setInt(2, Integer.parseInt(values[1]));
                preparedStmt.setString(3, values[0]);
                MID++;
                // Execute the SQL statement
                preparedStmt.executeUpdate();
            }
            preparedStmt.close();
            br.close();
            con.close();
            System.out.println("Data inserted successfully.");
        } catch (ClassNotFoundException e) {
            System.err.println("Class not found" + e.getMessage());
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Sql exception" + e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (br != null)
                    br.close();
                }
            catch(IOException e){
                throw new RuntimeException(e);
                }
            try{
            if (con != null)
                con.close();}
            catch (SQLException e){
                System.err.println("Sql exception" + e.getMessage());
                e.printStackTrace();
            }
            try{
                if (preparedStmt != null)
                    preparedStmt.close();}
            catch (SQLException e){
                System.err.println("Sql exception" + e.getMessage());
                e.printStackTrace();
            }


        }
        }

            // Method that calculates the similarity between all pairs of media items and stores the results in a database table
    public void calculateSimilarity(){
        Connection con = null;
        ResultSet rs = null;
        ResultSet rs2 = null;
        ResultSet rs3 = null;
        CallableStatement MaximalDistance = null;
        Statement st = null;
        PreparedStatement stmt = null;
        CallableStatement cstmt = null;
        PreparedStatement select_stmt = null;
        Statement st2 = null;

        try {
            // Define connection to SQL SERVER:
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            con = DriverManager.getConnection(this.connectionUrl);

            // Get the value of maximal_distance from a stored procedure
            MaximalDistance = con.prepareCall("{? = call dbo.MaximalDistance()}");
            MaximalDistance.registerOutParameter(1, Types.DOUBLE);
            MaximalDistance.execute();
            Integer maximalDistance = MaximalDistance.getInt(1);
            MaximalDistance.close();

            // Select all media items from the database
            String query = "select * from MediaItems;";
            st = con.createStatement();
            rs = st.executeQuery(query);

            // Iterate over all pairs of media items and calculate their similarity using a stored procedure
            while (rs.next()) {
                // get values from the current row
                Integer MID = rs.getInt("MID");

                // perform another query with values from the current row
                String query2 = "select * from MediaItems;";
                st2 = con.createStatement();
                rs2 = st2.executeQuery(query2);

                while (rs2.next()) {
                    // get values from the nested query
                    Integer MID2 = rs2.getInt("MID");

                    // Call a stored procedure to calculate the similarity between the current pair of media items
                    cstmt = con.prepareCall("{? = call dbo.SimCalculation(?,?,?)}");
                    cstmt.registerOutParameter(1, Types.REAL);
                    cstmt.setInt(2, MID);
                    cstmt.setInt(3, MID2);
                    cstmt.setDouble(4, maximalDistance);
                    cstmt.execute();
//
                    Double similarity = cstmt.getDouble(1);
                    String insert_query = "Insert into Similarity(MID1,MID2,SIMILARITY)\n" + "VALUES(?,?,?);";
                    stmt = con.prepareStatement(insert_query);
                    stmt.setInt(1, MID);
                    stmt.setInt(2, MID2);
                    stmt.setDouble(3, similarity);

                    // check if a record with the same primary key value already exists in the table
                    String select_query = "SELECT COUNT(*) FROM Similarity WHERE MID1 = ? AND MID2 = ?";
                    select_stmt = con.prepareStatement(select_query);
                    select_stmt.setInt(1, MID);
                    select_stmt.setInt(2, MID2);
                    rs3 = select_stmt.executeQuery();

                    if (rs3.next() && rs3.getInt(1) == 0) {
                        stmt.executeUpdate();
                    }
                    // close all open connections above
                    stmt.close();
                    cstmt.close();
                    rs3.close();
                    select_stmt.close();
                }
                rs2.close();
                st2.close();
            }

            rs.close();
            st.close();
            con.close();
        } catch (ClassNotFoundException e) {
            System.err.println("Class not found" + e.getMessage());
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Sql exception" + e.getMessage());
            e.printStackTrace();
        }
        finally {
            try{
                if (con != null)
                    con.close();
            } catch (SQLException e) {
                System.err.println("Sql exception" + e.getMessage());
                e.printStackTrace();
            }
            try {
                if (rs != null)
                    rs.close();
            } catch (SQLException e) {
                System.err.println("Sql exception" + e.getMessage());
                e.printStackTrace();
            }
            try {
                if (rs2 != null)
                    rs2.close();
            } catch (SQLException e) {
                System.err.println("Sql exception" + e.getMessage());
                e.printStackTrace();
            }
            try {
                if (rs3 != null)
                    rs3.close();
            } catch (SQLException e) {
                System.err.println("Sql exception" + e.getMessage());
                e.printStackTrace();
            }
            try {
                if (MaximalDistance != null)
                    MaximalDistance.close();
            } catch (SQLException e) {
                System.err.println("Sql exception" + e.getMessage());
                e.printStackTrace();
            }
            try {
                if (st != null)
                    st.close();
            } catch (SQLException e) {
                System.err.println("Sql exception" + e.getMessage());
                e.printStackTrace();
            }
            try {
                if (st2 != null)
                    st2.close();
            } catch (SQLException e) {
                System.err.println("Sql exception" + e.getMessage());
                e.printStackTrace();
            }
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException e) {
                System.err.println("Sql exception" + e.getMessage());
                e.printStackTrace();
            }
            try {
                if (cstmt != null)
                    cstmt.close();
            } catch (SQLException e) {
                System.err.println("Sql exception" + e.getMessage());
                e.printStackTrace();
            }
            try {
                if (select_stmt != null)
                    select_stmt.close();
            } catch (SQLException e) {
                System.err.println("Sql exception" + e.getMessage());
                e.printStackTrace();
            }


        }
    }

    public void printSimilarities(long mid){
        Connection con = null;
        ResultSet rs3 = null;
        ResultSet res_title1 = null;
        ResultSet res_title2 = null;
        PreparedStatement select_query = null;
        PreparedStatement select_stmt1 = null;
        PreparedStatement select_stmt2 = null;

        try {
            // Define connection to SQL SERVER:
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            con = DriverManager.getConnection(this.connectionUrl);

            //select all the rows where MID1=mid that is given in the func and SIMILARITY>=0.3 and orders it in ascending order.
            String query = "SELECT * FROM Similarity WHERE MID1=? and SIMILARITY >= 0.3 ORDER BY Similarity ASC;";
            select_query = con.prepareStatement(query);
            select_query.setLong(1, mid);
            rs3 = select_query.executeQuery();

            //Get the title of the mid given from MediaItems table
            String title_mid1 = "SELECT TITLE FROM MediaItems WHERE MID = ?";
            select_stmt1 = con.prepareStatement(title_mid1);
            select_stmt1.setInt(1, (int) mid);
            res_title1 = select_stmt1.executeQuery();
            res_title1.next();
            String title1 = res_title1.getString("TITLE");

            //iterating over all the couples of MID's that has a similarity of over 0.3 with the given mid
            while (rs3.next()) {
                int MID2 = rs3.getInt("MID2");
                double similarity = rs3.getDouble("SIMILARITY");


                String title_mid2 = "SELECT TITLE FROM MediaItems WHERE MID = ?";
                select_stmt2 = con.prepareStatement(title_mid2);
                select_stmt2.setInt(1, MID2);
                res_title2 = select_stmt2.executeQuery();
                res_title2.next();
                String title2 = res_title2.getString("TITLE");


                //printing the titles of the MID's and their similarities.
                System.out.println(title1 + " " + title2 + " " + Double.toString(similarity));
                select_stmt1.close();
                select_stmt2.close();
                res_title1.close();
                res_title2.close();
            }
            rs3.close();
            con.close();
        } catch (ClassNotFoundException e) {
            System.err.println("Class not found" + e.getMessage());
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Sql exception" + e.getMessage());
            e.printStackTrace();
        }
        finally {
            try{
                if (con != null)
                    con.close();
            } catch (SQLException e) {
                System.err.println("Sql exception" + e.getMessage());
                e.printStackTrace();
            }
            try {
                if (rs3 != null)
                    rs3.close();
            } catch (SQLException e) {
                System.err.println("Sql exception" + e.getMessage());
                e.printStackTrace();
            }

            try {
                if (res_title1 != null)
                    res_title1.close();
            } catch (SQLException e) {
                System.err.println("Sql exception" + e.getMessage());
                e.printStackTrace();
            }
        }
        try {
            if (res_title2 != null)
                res_title2.close();
        } catch (SQLException e) {
            System.err.println("Sql exception" + e.getMessage());
            e.printStackTrace();
        }
        try {
            if (select_query != null)
                select_query.close();
        } catch (SQLException e) {
            System.err.println("Sql exception" + e.getMessage());
            e.printStackTrace();
        }
        try {
            if (select_stmt1 != null)
                select_stmt1.close();
        } catch (SQLException e) {
            System.err.println("Sql exception" + e.getMessage());
            e.printStackTrace();
        }
        try {
            if (select_stmt2 != null)
                select_stmt2.close();
        } catch (SQLException e) {
            System.err.println("Sql exception" + e.getMessage());
            e.printStackTrace();
        }
    }


}




