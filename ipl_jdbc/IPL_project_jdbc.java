package ipl_jdbc;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.*;

public class IPL_project_jdbc {

	  // public Connection con;
	    public static void main(String[] args) throws Exception {
	    	noOfMatchesPlayedOfAllYears();
	    	noOfMatchesWonOfAllTeamsOverall();
	    	extraRunsConcededPerTeamIn2016();
	    	topEconomicalBowlerFor2015();
	    	temwithMaximumTossWin();
	   }

		public static void noOfMatchesPlayedOfAllYears() throws  Exception{
	        String url="jdbc:postgresql://localhost:5432/ipl";
	        String user="postgres";
	        String password="Manish10@";
	        String PSQLquery="SELECT season,count(*) AS noOfMatchPlayed\r\n" + 
	        		"FROM matches\r\n" + 
	        		"GROUP BY season\r\n" + 
	        		"ORDER BY season; ";
	        Class.forName("org.postgresql.Driver");
	        Connection con = DriverManager.getConnection(url,user,password);
	        Statement statement = con.createStatement();
	        ResultSet dataArray = statement.executeQuery(PSQLquery);
	        while(dataArray.next()) System.out.println(dataArray.getString("season")+": "+ dataArray.getInt("noOfMatchPlayed"));
	        statement.close();
	        con.close();
	    }
	    
	    public static void noOfMatchesWonOfAllTeamsOverall() throws  Exception{
	        String url="jdbc:postgresql://localhost:5432/ipl";
	        String user="postgres";
	        String password="Manish10@";
	        String pSQLquery="SELECT winner AS team,COUNT(winner) AS totalMatchWon\r\n" + 
	        		"FROM matches\r\n" + 
	        		"GROUP BY team\r\n" + 
	        		"ORDER BY totalMatchWon DESC\r\n" + 
	        		"LIMIT 14;";
	        Class.forName("org.postgresql.Driver");
	        Connection con = DriverManager.getConnection(url,user,password);
	        Statement statement = con.createStatement();
	        ResultSet dataArray = statement.executeQuery(pSQLquery);
	        while(dataArray.next())System.out.println(dataArray.getString("team")+" :"+ dataArray.getInt("totalMatchWon"));
	        statement.close();
	        con.close();
	    }
	    
	    public  static void extraRunsConcededPerTeamIn2016() throws  Exception{
	        String url="jdbc:postgresql://localhost:5432/ipl";
	        String user="postgres";
	        String password="Manish10@";
	        Class.forName("org.postgresql.Driver");
	        Connection con = DriverManager.getConnection(url,user,password);
	        Statement statement = con.createStatement();
	        String PSQLquery="SELECT bowling_team AS team,SUM(extra_runs) AS extraRunsConceded\r\n" + 
	        		"FROM deleveries\r\n" + 
	        		"WHERE match_id IN (SELECT id FROM matches\r\n" + 
	        		"WHERE season='2016')\r\n" + 
	        		"GROUP BY team";
	        ResultSet dataArray = statement.executeQuery(PSQLquery);
	        while(dataArray.next()) {
	            System.out.println(dataArray.getString("team")+":"+ dataArray.getInt("extraRunsConceded"));
	        }
	        statement.close();
	        con.close();
	    }
	    
	    public  static  void  topEconomicalBowlerFor2015() throws Exception{
	        String url="jdbc:postgresql://localhost:5432/ipl";
	        String user="postgres";
	        String pass="Manish10@";
	        Class.forName("org.postgresql.Driver");
	        Connection con = DriverManager.getConnection(url,user,pass);
	        Statement statement = con.createStatement();
	        String PSQLquery="SELECT bowler AS bowlerName,SUM(total_runs)/(COUNT(bowler)/6.0) AS BowlerEconomy\r\n" + 
	        		"FROM deleveries\r\n" + 
	        		"WHERE match_id IN (SELECT id FROM matches\r\n" + 
	        		"WHERE season='2015')\r\n" + 
	        		"GROUP BY bowlerName\r\n" + 
	        		"ORDER BY BowlerEconomy\r\n" + 
	        		"LIMIT 10";
	        ResultSet dataArray = statement.executeQuery(PSQLquery);
	        while(dataArray.next())System.out.println(dataArray.getString("bowlerName")+" : "+ dataArray.getFloat("BowlerEconomy"));
	        statement.close();
	        con.close();
	    }
	    
		public static void temwithMaximumTossWin() throws  Exception{
	        String url="jdbc:postgresql://localhost:5432/ipl";
	        String user="postgres";
	        String password="Manish10@";
	        String PSQLquery="SELECT toss_winner AS tossWinner,COUNT(*) AS NoOfTimesTossWon\r\n" + 
	        		"FROM matches\r\n" + 
	        		"GROUP BY tossWinner\r\n" + 
	        		"ORDER BY tossWinner\r\n" + 
	        		"LIMIT 1";
	        Class.forName("org.postgresql.Driver");
	        Connection con = DriverManager.getConnection(url,user,password);
	        Statement statement = con.createStatement();
	        ResultSet dataArray = statement.executeQuery(PSQLquery);
	        while(dataArray.next()) System.out.println(dataArray.getString("tossWinner")+" : "+ dataArray.getInt("NoOfTimesTossWon"));
	        statement.close();
	        con.close();
	    }
	    
}

