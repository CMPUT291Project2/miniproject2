package project2;

import java.io.FileNotFoundException;
import java.util.Random;

import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseType;



public class Main {

	/**
	 * @param args
	 */
	private static final String DB_TABLE = "ed_table";
	private static final int NO_RECORDS = 1000;
	private static String db_type_option;
	private static Database my_table;
	private static boolean exit = false;
	private static int selection;

	public static void main(String[] args) throws FileNotFoundException, DatabaseException {
		// Retrieve db type option
		db_type_option = args[0];
		while(shouldNotExit()) {
			// Create Interface
			try {
				System.console().printf("\nMain Menu\n\n");
				selection = Integer.parseInt(System.console().readLine("1) Create and Populate Database\n" +
						"2) Retrieve Records with a given key\n" +
						"3) Retrieve Records with given data\n" +
						"4) Retrieve Records with a given range of keys\n" +
						"5) Destroy Database\n" +
						"6) Quit\n\nPlease Enter Selection Number: "));
			} catch (NumberFormatException e) {
				System.out.println("Invalid Input!\n");
				continue;
			}
			performSelection(selection);
		}
	}

	private static void performSelection(int selection) throws FileNotFoundException, DatabaseException {
		switch(selection) {
		case 1:
			createPopulateDB(db_type_option);
			System.out.println("Creating Database...");
			break;
		case 2:
			// TODO
			System.out.println("Retrieve Records with Given Key");
			break;
		case 3:
			// TODO
			System.out.println("Retrieve Records with Given Data");
			break;
		case 4:
			// TODO
			System.out.println("Retrieve Records with Given Range of Keys");
			break;
		case 5:
			System.out.println("Destroying Database...");
			destroyDB();
			break;
		case 6:
			exit=true;
			return;
		default:
			System.out.println("Invalid Selection\n");
			return;
		}

	}

	// Select 1: Create and Populate Database
	public static void createPopulateDB(String db_type) throws FileNotFoundException, DatabaseException {
		DatabaseConfig dbConfig = new DatabaseConfig();
		boolean input_err = true;
		while(input_err) {
			if (db_type.equals("btree")) {
				dbConfig.setType(DatabaseType.BTREE);
				input_err = false;
				break;
			} else if (db_type.equals("hash")) {
				dbConfig.setType(DatabaseType.HASH);
				input_err = false;
				break;
			} else if (db_type.equals("indexfile")) {
				System.out.println("Index File option not implemented yet");
			} else {
				System.out.println("Invalid Input, try again...");
				continue;
			}
		}
		dbConfig.setAllowCreate(true);
		my_table = new Database(DB_TABLE, null, dbConfig);
		System.out.println(DB_TABLE + " has been created");
		System.out.println("Database Type: " + dbConfig.getType().toString());
		System.out.println();
		System.out.println("Populating database...");
		populateDB(my_table,NO_RECORDS);
	}

	// This populate database function is borrows from the provided java sample code
	// Reference: https://eclass.srv.ualberta.ca/pluginfile.php/1930632/mod_assign/intro/Sample.java
	private static void populateDB(Database my_table2, int numRecords) {
		int range;
		DatabaseEntry kdbt, ddbt;
		String s;

		/*  
		 *  generate a random string with the length between 64 and 127,
		 *  inclusive.
		 *
		 *  Seed the random number once and once only.
		 */
		Random random = new Random(1000000);
		System.out.println("Random Num: " + random);
		try {
			for (int i = 0; i < numRecords; i++) {

				/* to generate a key string */
				range = 64 + random.nextInt( 64 );
				s = "";
				for ( int j = 0; j < range; j++ ) 
					s+=(new Character((char)(97+random.nextInt(26)))).toString();
				System.out.println("Key: " + s);
				
				/* to create a DBT for key */
				kdbt = new DatabaseEntry(s.getBytes());
				kdbt.setSize(s.length()); 

				// to print out the key/data pair
				// System.out.println(s);	

				/* to generate a data string */
				range = 64 + random.nextInt( 64 );
				s = "";
				for ( int j = 0; j < range; j++ ) 
					s+=(new Character((char)(97+random.nextInt(26)))).toString();
				// to print out the key/data pair
				System.out.println("Data: " + s);	
				// System.out.println("");

				/* to create a DBT for data */
				ddbt = new DatabaseEntry(s.getBytes());
				ddbt.setSize(s.length()); 

				/* to insert the key/data pair into the database */
				my_table.putNoOverwrite(null, kdbt, ddbt);
			}
		}
		catch (DatabaseException dbe) {
			System.err.println("Populate the table: "+dbe.toString());
			System.exit(1);
		}

	}


	public static void destroyDB() throws FileNotFoundException, DatabaseException {
		/* close the database and the database environment */
		my_table.close();
		/* to remove the table */
		my_table.remove(DB_TABLE,null,null);
	}

	private static boolean shouldNotExit() {
		return !exit;
	}
}
