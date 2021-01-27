

import java.util.*;
import java.net.*;
import java.text.*;
import java.lang.*;
import java.io.*;
import java.sql.*;
import pgpass.*;
import java.util.Date;


public class CreateQuest {


    	    private Connection conDB;        // Connection to the database system.
    	    private String url;              // URL: Which database?
            private String user = "akifp";	// user id

    	    private String date;     // check for the date
    	    private String  realm;   // realm name.
    	    private String theme ;   // theme check
    	    private int amount; 	// amount check
    	    private String seed = null;

    	    // Constructor
    	    public CreateQuest (String[] args)  {


    	        // Set up the DB connection.
    	        try {
    	            // Register the driver with DriverManager.
    	            Class.forName("org.postgresql.Driver").newInstance();
    	        } catch (ClassNotFoundException e) {
    	            e.printStackTrace();
    	            System.exit(0);
    	        } catch (InstantiationException e) {
    	            e.printStackTrace();
    	            System.exit(0);
    	        } catch (IllegalAccessException e) {
    	            e.printStackTrace();
    	            System.exit(0);
    	        }


    	        // URL: Which database?
    	       
              url = "jdbc:postgresql://db:5432/";

              // set up acct info
              // fetch the PASSWD from <.pgpass> a file in the respective directory which stores the password to db
              Properties props = new Properties();
              try {
                  String passwd = PgPass.get("db", "*", user, user);
                  props.setProperty("user",    "akifp");
                  props.setProperty("password", passwd);
                  // props.setProperty("ssl","true"); // NOT SUPPORTED on DB
              } catch(PgPassException e) {
                  System.out.print("\nCould not obtain PASSWD from <.pgpass>.\n");
                  System.out.println(e.toString());
                  System.exit(0);
              }



    	        // Initialize the connection.
    	        try {
    	            // Connect with a fall-thru id & password
    	            conDB = DriverManager.getConnection(url,props);
    	            System.out.println("Connection successful!");
    	        } catch(SQLException e) {
    	            System.out.print("\nSQL: database connection error.\n");
    	            System.out.println(e.toString());
    	            System.exit(0);
    	        }

    	        // Let's have autocommit turned off.  No particular reason here.
    	        try {
    	            conDB.setAutoCommit(false);
    	        } catch(SQLException e) {
    	            System.out.print("\nFailed trying to turn autocommit off.\n");
    	            e.printStackTrace();
    	            System.exit(0);
    	        }


          //check if the given date is in future
    	        date = args[0];
    	        SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
    	        Date dt = new Date();
      				try {
      					dt = sdformat.parse(date);
      				} catch (ParseException e) {
      					// TODO Auto-generated catch block
      					e.printStackTrace();
      				}

    	        if(!checkDate(dt)) {
    	        	System.out.println("Date is not in future");
                System.exit(0);
    	        }


         // check realm
               realm = args[1];
               if(!checkRealm(realm)) {
                   System.out.println(realm + " is not in the realm");
                   System.exit(0);
               }


         // check amount
               amount = Integer.parseInt(args[3]);
               if(!checkAmount(amount)) {
                 System.out.println(amount + " is greater than total sql amount");
                 System.exit(0);
               }

               // adding the quest
               theme = args[2];
               add_Quest(theme, realm, dt);

           		// check seed value and call the add_loot method
           		if (args.length == 6) {
                seed = args[5];
           			Float seedCheck = Float.parseFloat(args[5]);
           			if ((seedCheck < -1.0) && (seedCheck > 1.0)) {
           				System.out.println("Seed is not in range");
           				System.exit(0);
           			}
           		}

              //adding into loot
           		add_loot(theme, realm, dt, seed, amount);

        }


//================================================error check methods start here================================

        // method to check date
    	    public boolean checkDate(Date date) {

    	    	Date dt = new Date();

    	    	if(date.compareTo(dt)>0) {
    	    		return true;
    	    	}
    	    	return false;
    	    }

        // method to check realm
    	    public boolean checkRealm(String realm) {

    	        String            queryText = "";     // The SQL text.
    	        PreparedStatement querySt   = null;   // The query handle.
    	        ResultSet         answers   = null;   // A cursor.

    	        boolean           bool      = false;  // Return.

    	        queryText =
    	            "SELECT realm       "
    	          + "FROM realm "
    	          + "WHERE LOWER(realm) = ?     ";

    	        // Prepare the query.
    	        try {
    	            querySt = conDB.prepareStatement(queryText);
    	        } catch(SQLException e) {
    	            System.out.println("realm query failed in prepare");
    	            System.out.println(e.toString());
    	            System.exit(0);
    	        }

    	        // Execute the query.
    	        try {
    	            querySt.setString(1, realm.toLowerCase());
    	            answers = querySt.executeQuery();
    	        } catch(SQLException e) {
    	            System.out.println("realm query failed in execute");
    	            System.out.println(e.toString());
    	            System.exit(0);
    	        }

    	        // Any answer?
    	        try {
    	            if (answers.next()) {
    	                bool = true;
    	            } else {
    	                bool = false;
    	            }
    	        } catch(SQLException e) {
    	            System.out.println("realm failed during ResultSet answers check.");
    	            System.out.println(e.toString());
    	            System.exit(0);
    	        }

    	        // Close the cursor.
    	        try {
    	            answers.close();
    	        } catch(SQLException e) {
    	            System.out.print("realm query closing cursor failed.\n");
    	            System.out.println(e.toString());
    	            System.exit(0);
    	        }

    	        // We're done with the handle.
    	        try {
    	            querySt.close();
    	        } catch(SQLException e) {
    	            System.out.print("realm failed during closing the handle.\n");
    	            System.out.println(e.toString());
    	            System.exit(0);
    	        }

    	        return bool;
    	    }


    	// method to check amount
    	    public boolean checkAmount(int amount) {
    	        String            queryText = "";     // The SQL text.
    	        PreparedStatement querySt   = null;   // The query handle.
    	        ResultSet         answers   = null;   // A cursor.

    	        boolean           inDB      = false;  // Return.

    	        queryText =
    	            "SELECT sum(sql)       "
    	          + "FROM treasure "
    	          + "HAVING sum(sql)>=?  "  		;

    	        // Prepare the query.
    	        try {
    	            querySt = conDB.prepareStatement(queryText);
    	        } catch(SQLException e) {
    	            System.out.println("checkAmount query failed in prepare");
    	            System.out.println(e.toString());
    	            System.exit(0);
    	        }

    	        // Execute the query.
    	        try {
    	        	querySt.setInt(1, amount);
    	            answers = querySt.executeQuery();
    	        } catch(SQLException e) {
    	            System.out.println("checkAmount query failed in execute");
    	            System.out.println(e.toString());
    	            System.exit(0);
    	        }

    	        // Any answer?
    	        try {

    	            if (answers.next()) {
    	                inDB = true;
    	            } else {
    	                inDB = false;
    	            }
    	        } catch(SQLException e) {
    	            System.out.println("checkAmount failed in cursor.");
    	            System.out.println(e.toString());
    	            System.exit(0);
    	        }

    	        // Close the cursor.
    	        try {
    	            answers.close();
    	        } catch(SQLException e) {
    	            System.out.print("checkAmount failed closing cursor.\n");
    	            System.out.println(e.toString());
    	            System.exit(0);
    	        }

    	        // We're done with the handle.
    	        try {
    	            querySt.close();
    	        } catch(SQLException e) {
    	            System.out.print("checkAmount failed closing the handle.\n");
    	            System.out.println(e.toString());
    	            System.exit(0);
    	        }

    	        return inDB;
    	    }

          // adding tuple to quest
          	    public void add_Quest(String theme,String realm, Date day) {


          	    	java.sql.Date sqlDate = new java.sql.Date(day.getTime());
          	    	String            queryText = "";     // The SQL text.
          	        PreparedStatement querySt   = null;   // The query handle.
          	        int         answers = 0;   // A cursor.


          	        queryText =
          	            "INSERT INTO quest       "
          	          + "VALUES(?,?,?)			 "	;

          	        // Prepare the query.
          	        try {
          	            querySt = conDB.prepareStatement(queryText);
          	        } catch(SQLException e) {
          	            System.out.println("insert into Quest query failed in prepare");
          	            System.out.println(e.toString());
          	            System.exit(0);
          	        }

          	        // Execute the query.
          	        try {
          	            querySt.setString(1, theme);
          	            querySt.setString(2, realm);
          	            querySt.setDate(3, sqlDate);
          	            answers = querySt.executeUpdate();
          	        } catch(SQLException e) {
          	            System.out.println("Quest query failed in execute");
          	            System.out.println(e.toString());
          	            System.exit(0);
          	        }


          	     // Commit.  Okay, here nothing to commit really, but why not...
          	        try {
          	            conDB.commit();
          	        } catch(SQLException e) {
          	            System.out.print("\n Failed trying to commit Quest tuple.\n");
          	            e.printStackTrace();
          	            System.exit(0);
          	        }


          	        // We're done with the handle.
          	        try {
          	            querySt.close();
          	        } catch(SQLException e) {
          	            System.out.print("Quest tuple failed closing the handle.\n");
          	            System.out.println(e.toString());
          	            System.exit(0);
          	        }

     }


	// method to add loot
  public void add_loot(String theme, String realm, Date day, String seed, int amount) {

		java.sql.Date sqlDate = new java.sql.Date(day.getTime());
		int loot_id = 1;            //used as a loot_id while inserting

		String queryTextSeed = ""; // The SQL text for seed.
		String queryTextSql = ""; // SQL text for looping over treasure to keep a track of total sql
		String queryTextLoot = ""; // SQL text for inserting into loot

		PreparedStatement queryStSeed = null; // The query handle .
		PreparedStatement queryStSql = null; // The query handle.
		PreparedStatement queryStLoot = null; // The query handle.


		ResultSet answersSql = null; // cursor for keeping track of total sql

		queryTextSeed = "SELECT setseed(?)";

		queryTextSql = "SELECT * 				 	" +
					   "   FROM Treasure 			" +
					   "   		   ORDER by random()";

		queryTextLoot = "INSERT INTO loot 			" +
						" 		   VALUES(?,?,?,?,?)";


	// Prepare the query.
		try {
			queryStSeed = conDB.prepareStatement(queryTextSeed);
			queryStSql = conDB.prepareStatement(queryTextSql);
			queryStLoot = conDB.prepareStatement(queryTextLoot);

		} catch (SQLException e) {
			System.out.println("insert into loot query 1 failed in prepare");
			System.out.println(e.toString());
			System.exit(0);
		}


	// Execute the seed and looping over treasure query.
		try {
			if(seed!=null) {
				queryStSeed.setFloat(1, Float.parseFloat(seed));
				answersSql = queryStSeed.executeQuery();
			}

				answersSql = queryStSql.executeQuery();

		} catch (SQLException e) {
			System.out.println("Seed or looping over treasure query failed insert into loot while executing");
			System.out.println(e.toString());
			System.exit(0);
		}


	// execute the insert into loot query
		int sumCheck = 0;

		try {
			while (answersSql.next() && sumCheck<=amount) {

				sumCheck += answersSql.getInt("sql");

  				try {
  					queryStLoot.setInt(1, loot_id);
  					queryStLoot.setString(2, answersSql.getString("treasure"));
  					queryStLoot.setString(3, theme);
  					queryStLoot.setString(4, realm);
  					queryStLoot.setDate(5, sqlDate);
  					loot_id++;
  					queryStLoot.executeUpdate();

  				} catch (SQLException e) {
  					System.out.println("insert loot query failed in execute");
  					System.out.println(e.toString());
  					System.exit(0);
  				}

        }//while loop ends here

			} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			}

		// Commit.
		try {
			conDB.commit();
		} catch (SQLException e) {
			System.out.print("\nFailed trying to commit.\n");
			e.printStackTrace();
			System.exit(0);
		}

		// Close the connection.
		try {
			conDB.close();
		} catch (SQLException e) {
			System.out.print("\nFailed trying to close the connection.\n");
			e.printStackTrace();
			System.exit(0);
		}

		// Close the cursor.
		  try {
			  answersSql.close();
			  } catch(SQLException e) {
				  System.out.print("ResultSet answersSql failed while closing cursor.\n");
				  System.out.println(e.toString());
				  System.exit(0);
				  }


		// We're done with the handle.
			try {
				queryStSeed.close();
				queryStSql.close();
				queryStLoot.close();
			} catch (SQLException e) {
				System.out.print("inserting into loot query failed while closing the handle.\n");
				System.out.println(e.toString());
				System.exit(0);
			}

	}


// main method
    	    public static void main(String[] args) throws ParseException  {
    	    	CreateQuest ct = new CreateQuest(args);
    	    }

}
