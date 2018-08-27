import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * Runs queries against a back-end database
 */
public class Query
{
  private String configFilename;
  private Properties configProps = new Properties();

  private String jSQLDriver;
  private String jSQLUrl;
  private String jSQLUser;
  private String jSQLPassword;

  // DB Connection
  private Connection conn;

  // Logged In User
  private String username = null; // customer username is unique
  private boolean loggedIn = false;
  private boolean wasLoggedInWhenSearched = false;
  
  // Holds itineraries for most recent search
  private List<Itinerary> searchedItineraries = new ArrayList<Itinerary>();

  // Canned queries

  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  private PreparedStatement checkFlightCapacityStatement;

  // transactions
  private static final String BEGIN_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
  private PreparedStatement beginTransactionStatement;

  private static final String COMMIT_SQL = "COMMIT TRANSACTION";
  private PreparedStatement commitTransactionStatement;

  private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
  private PreparedStatement rollbackTransactionStatement;
  
  // My prepared statements
  
  private static final String FLIGHT_SEARCH =
  "SELECT TOP (?) fid, year, carrier_id, flight_num, actual_time, capacity, price "
		  + "FROM Flights WITH (TABLOCKX) "
		  + "WHERE origin_city = ? AND dest_city = ? AND day_of_month = ? AND actual_time IS NOT NULL "
		  + "ORDER BY actual_time, fid ASC";
  private PreparedStatement flightSearchStatement;
  
  private static final String FLIGHT_SEARCH_2 =
  "SELECT TOP (?) f.fid AS fid1, f2.fid AS fid2, f.year AS year1, f2.year AS year2, f.carrier_id AS cid1, f2.carrier_id AS cid2, "
  + "f.flight_num AS flight_num1, f2.flight_num AS flight_num2, f.dest_city AS middle_city, f.actual_time AS time1, f2.actual_time AS time2, " 
  + "f.capacity AS capacity1, f2.capacity AS capacity2, f.price AS price1, f2.price AS price2 "
  + "FROM Flights f, Flights f2 WITH (TABLOCKX) "
  + "WHERE f.origin_city = ? AND f.dest_city = f2.origin_city AND f2.dest_city = ? AND f.day_of_month = ? AND f2.day_of_month = ? AND f.actual_time IS NOT NULL AND f2.actual_time IS NOT NULL "
  + "ORDER BY (f.actual_time + f2.actual_time), f.fid, f2.fid ASC";
  
  private PreparedStatement flightSearch2Statement;
  
  private static final String USERNAME_SEARCH = "SELECT u.username FROM Users AS u WITH (TABLOCKX) WHERE ? = u.username";
  private PreparedStatement usernameSearchStatement;
  
  private static final String USERNAME_INSERT = "INSERT INTO Users WITH (TABLOCKX) (username, password, balance) VALUES (?, ?, ?) ";
  private PreparedStatement usernameInsertStatement;
  
  private static final String RESERVATION_INSERT = "INSERT INTO Reservations (id, username, fid1, fid2, paid)"
			+ " VALUES (?, ?, ?, ?, ?)";
  private PreparedStatement reservationInsertStatement;
  
  private static final String RESERVATION_CHECK_SAME_DAY = "SELECT fid1, fid2 FROM Reservations WITH (TABLOCKX) "
  														 + " WHERE username = ?";
  private PreparedStatement reservationSameDayCheck;
  
  private static final String GET_RESERVATION_FLIGHT_NUMS = "SELECT fid1, fid2, id, paid FROM Reservations WITH (TABLOCKX) WHERE username = ?";
  private PreparedStatement reservationFlightNumsStatement;
  
  private static final String RESERVATION_EXISTS = "SELECT fid1, fid2, paid FROM Reservations WITH (TABLOCKX) WHERE username = ? AND id = ?";
  private PreparedStatement reservationExists;
  
  private static final String FLIGHT_PRICE = "SELECT price FROM Flights WITH (TABLOCKX) WHERE fid = ?";
  private PreparedStatement flightPrice;
  
  private static final String REFUND = "UPDATE Users WITH (TABLOCKX) SET balance = (balance + ?) WHERE username = ?";
  private PreparedStatement refundStatement;
  
  private static final String DELETE_RESERVATION = "DELETE FROM RESERVATIONS WITH (TABLOCKX) WHERE id = ?";
  private PreparedStatement deleteReservationStatement;
  
  private static final String GET_BALANCE = "SELECT balance FROM Users WITH (TABLOCKX) WHERE username = ?";
  private PreparedStatement getBalanceStatement;
  
  private static final String PAY = "UPDATE Users WITH (TABLOCKX) SET balance = (balance - ?) WHERE username = ?";
  private PreparedStatement payStatement;

  class Flight
  {
    public int fid;
    public int year;
    public int monthId;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public double time;
    public int capacity;
    public double price;

    @Override
    public String toString()
    {
      return "ID: " + fid + " Date: " + year + "-" + monthId + "-" + dayOfMonth + " Carrier: " + carrierId +
              " Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
              " Capacity: " + capacity + " Price: " + price;
    }
  }
  
  // Holds 1 to 2 flights. If one hop then 1 flight and null, if two hop then two flights
  class Itinerary {
	  public Flight f1;
	  public Flight f2;
	  
	  public Itinerary(Flight f1, Flight f2) {
		  this.f1 = f1;
		  this.f2 = f2;
	  }
	  
	  public Itinerary(Flight f1) {
		  this.f1 = f1;
		  this.f2 = null;
	  }
	  
	  @Override
	  public String toString() {
		  return this.f1.toString() + "\n" + f2.toString();
	  }
  }
  
  class Pair {
	  
	  public int id;
	  public boolean paid;
	  
	  public Pair(int id, boolean paid) {
		  this.id = id;
		  this.paid = paid;
	  }
  }

  public Query(String configFilename)
  {
    this.configFilename = configFilename;
  }

  /* Connection code to SQL Azure.  */
  public void openConnection() throws Exception
  {
    configProps.load(new FileInputStream(configFilename));

    jSQLDriver = configProps.getProperty("flightservice.jdbc_driver");
    jSQLUrl = configProps.getProperty("flightservice.url");
    jSQLUser = configProps.getProperty("flightservice.sqlazure_username");
    jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

		/* load jdbc drivers */
    Class.forName(jSQLDriver).newInstance();

		/* open connections to the flights database */
    conn = DriverManager.getConnection(jSQLUrl, // database
            jSQLUser, // user
            jSQLPassword); // password

    conn.setAutoCommit(true); //by default automatically commit after each statement

		/* You will also want to appropriately set the transaction's isolation level through:
		   conn.setTransactionIsolation(...)
		   See Connection class' JavaDoc for details.
		 */
  }

  public void closeConnection() throws Exception
  {
    conn.close();
  }

  /**
   * Clear the data in any custom tables created. Do not drop any tables and do not
   * clear the flights table. You should clear any tables you use to store reservations
   * and reset the next reservation ID to be 1.
   */
  public void clearTables ()
  {
	  String clearTables = "DELETE FROM Users \n"
	  					 + "DELETE FROM Reservations \n"
	  					 + "DELETE FROM ResID";
	  
      try {
    	  
	    	Statement clearStatement = conn.createStatement();
			clearStatement.executeUpdate(clearTables);
			
			resetResID();
			
      } catch (SQLException e) {e.printStackTrace();}
  }
  
  private void resetResID() {
	  try {
		  Statement resetStatement = conn.createStatement();
		  resetStatement.executeUpdate("DELETE FROM ResID");
		  resetStatement.executeUpdate("INSERT INTO ResID (currID) VALUES (0)");
	  } catch (SQLException e) {e.printStackTrace();}
  }

	/**
   * prepare all the SQL statements in this method.
   * "preparing" a statement is almost like compiling it.
   * Note that the parameters (with ?) are still not filled in
   */
  public void prepareStatements() throws Exception
  {
    beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
    commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
    rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);

    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);

    /* add here more prepare statements for all the other queries you need */
	flightSearchStatement  = conn.prepareStatement(FLIGHT_SEARCH);
	flightSearch2Statement = conn.prepareStatement(FLIGHT_SEARCH_2);
	usernameSearchStatement = conn.prepareStatement(USERNAME_SEARCH);
	usernameInsertStatement = conn.prepareStatement(USERNAME_INSERT);
	reservationInsertStatement = conn.prepareStatement(RESERVATION_INSERT);
	reservationSameDayCheck = conn.prepareStatement(RESERVATION_CHECK_SAME_DAY);
	reservationFlightNumsStatement = conn.prepareStatement(GET_RESERVATION_FLIGHT_NUMS);
	reservationExists = conn.prepareStatement(RESERVATION_EXISTS);
	flightPrice = conn.prepareStatement(FLIGHT_PRICE);
	refundStatement = conn.prepareStatement(REFUND);
	deleteReservationStatement = conn.prepareStatement(DELETE_RESERVATION);
	getBalanceStatement = conn.prepareStatement(GET_BALANCE);
	payStatement = conn.prepareStatement(PAY);
  }

  /**
   * Takes a user's username and password and attempts to log the user in.
   *
   * @param username
   * @param password
   *
   * @return If someone has already logged in, then return "User already logged in\n"
   * For all other errors, return "Login failed\n".
   *
   * Otherwise, return "Logged in as [username]\n".
   */
  public String transaction_login(String username, String password)
  {
	  if (loggedIn) return "User already logged in\n";
	  
	  String checkIfUserExists = "SELECT username, password FROM Users";
	  
	  try {
		  Statement searchStatement = conn.createStatement();
		  ResultSet currUsernames = searchStatement.executeQuery(checkIfUserExists);
		  
		  while (currUsernames.next()) {
			  String currUsername = currUsernames.getString("username");
			  String currPassword = currUsernames.getString("password");
			  if (currUsername.equals(username)) {
				  if (currPassword.equals(password)) {
					  this.username = currUsername;
					  loggedIn = true;
					  return "Logged in as " + this.username + "\n";
				  }
			  }
		  }
		  currUsernames.close();
	} catch (SQLException e) {e.printStackTrace();}  
	  	  
	  return "Login failed\n";
  }

  /**
   * Implement the create user function.
   *
   * @param username new user's username. User names are unique the system.
   * @param password new user's password.
   * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure otherwise).
   *
   * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
   */
  public String transaction_createCustomer (String username, String password, double initAmount)
  {
	String failed = "Failed to create user\n";
	String success = "Created user " + username + "\n";
	
	if (initAmount < 0) return failed;
	if (username.length() > 20 || password.length() > 20) {
		System.out.println("BAD LENGTH\n");
		return failed;
	}
	
	try {
		usernameSearchStatement.setString(1, username);
		ResultSet possibleUser = usernameSearchStatement.executeQuery();
		
		if (possibleUser.next()) { 
			return failed;
		}
		
		possibleUser.close();
		
		usernameInsertStatement.setString(1, username);
		usernameInsertStatement.setString(2, password);
		usernameInsertStatement.setFloat(3, (float) initAmount);

		usernameInsertStatement.executeUpdate();
		
		return success;
		
	} catch (SQLException e) {e.printStackTrace();}
	
	return failed;
  }

  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination
   * city, on the given day of the month. If {@code directFlight} is true, it only
   * searches for direct flights, otherwise is searches for direct flights
   * and flights with two "hops." Only searches for up to the number of
   * itineraries given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight if true, then only search for direct flights, otherwise include indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your selection\n".
   * If an error occurs, then return "Failed to search\n".
   *
   * Otherwise, the sorted itineraries printed in the following format:
   *
   * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n
   * [first flight in itinerary]\n
   * ...
   * [last flight in itinerary]\n
   *
   * Each flight should be printed using the same format as in the {@code Flight} class. Itinerary numbers
   * in each search should always start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
                                   int numberOfItineraries)
  {
	  // Clear the previous itinerary search
	  searchedItineraries.clear();
	  
	  // Helps for case where they search, then login, then try to book
	  wasLoggedInWhenSearched = loggedIn;
	  
      return transaction_search_safe(originCity, destinationCity, directFlight, dayOfMonth, numberOfItineraries);
  }

  /**
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight
   * @param dayOfMonth
   * @param numberOfItineraries
   *
 * @throws SQLException 
   */
  
  private String transaction_search_safe(String originCity, String destinationCity, boolean directFlight, int dayOfMonth, int numberOfItineraries) {
	  
    StringBuffer sb = new StringBuffer();
	
    try { 
    	
    	 // Finds as many direct flights up to numberOfItineraries
    	 List<Flight> oneHopFlights = oneFlightHop(originCity, destinationCity, directFlight, dayOfMonth, numberOfItineraries);
    	 
    	 if (directFlight && oneHopFlights.size() == 0) return "No flights match your selection\n";
    	 
		 for (int i = 0; i < oneHopFlights.size(); i++) {
			 
			 Flight f = oneHopFlights.get(i);
			 
			 sb.append("Itinerary " + i + ": 1 flight(s), " + f.time + " minutes\n");
			 sb.append("ID: " + f.fid + " Date: " + f.year + "-7-" + f.dayOfMonth + " Carrier: " + 
			         f.carrierId + " Number: " + f.flightNum + " Origin: " + f.originCity + " Dest: " + 
					 f.destCity + " Duration: " + f.time + " Capacity: " + f.capacity + " Price: " + f.price + "\n");
		 }
		 
		 int numDirectItineraries = oneHopFlights.size();
		 
		 // If we haven't found enough flights, then we find non-direct flights up to newNumOfItineraries, if they wanted it
		 
		 if (!directFlight && numDirectItineraries < numberOfItineraries) {
			 int newNumOfItineraries = numberOfItineraries - numDirectItineraries;
			 List<Flight[]> twoHopFlights = twoFlightHop(originCity, destinationCity, directFlight, dayOfMonth, newNumOfItineraries);
			 
			 if (twoHopFlights.size() == 0 && oneHopFlights.size() == 0) return "No flights match your selection\n";		 
			 
			 for (int i = 0; i < newNumOfItineraries; i++) {
				 Flight[] f = twoHopFlights.get(i);
				 
				 sb.append("Itinerary " + (i + numDirectItineraries) + ": 2 flight(s), " + (f[1].time + f[2].time) + " minutes\n");
				 
				 sb.append("ID: " + f[1].fid + " Date: " + f[1].year + "-7-" + f[1].dayOfMonth + " Carrier: " + 
				         f[1].carrierId + " Number: " + f[1].flightNum + " Origin: " + f[1].originCity + " Dest: " + 
						 f[1].destCity + " Duration: " + f[1].time + " Capacity: " + f[1].capacity + " Price: " + f[1].price + "\n");
				 
				 sb.append("ID: " + f[2].fid + " Date: " + f[2].year + "-7-" + f[2].dayOfMonth + " Carrier: " + 
				         f[2].carrierId + " Number: " + f[2].flightNum + " Origin: " + f[2].originCity + " Dest: " + 
						 f[2].destCity + " Duration: " + f[2].time + " Capacity: " + f[2].capacity + " Price: " + f[2].price + "\n");
			 }
		 }
	} catch (SQLException e) {e.printStackTrace();}
	
	return sb.toString();
  }
  
  
  private List<Flight> oneFlightHop (String originCity, String destinationCity, boolean directFlight, int dayOfMonth, int numberOfItineraries) throws SQLException {
	  
	    flightSearchStatement.setInt(1, numberOfItineraries);
	    flightSearchStatement.setString(2, originCity);
	    flightSearchStatement.setString(3, destinationCity);
	    flightSearchStatement.setInt(4, dayOfMonth);
	    
	    ResultSet oneHopResults = flightSearchStatement.executeQuery();
	    
	    List<Flight> flights = new ArrayList<Flight>();

	    while (oneHopResults.next()) {	
	      Flight f = new Flight();
	      f.fid = oneHopResults.getInt("fid");
	      f.year = oneHopResults.getInt("year");
	      f.monthId = 7;
	      f.dayOfMonth = dayOfMonth;
	      f.carrierId = oneHopResults.getString("carrier_id");
	      f.flightNum = oneHopResults.getString("flight_num");
	      f.time = oneHopResults.getDouble("actual_time");
	      f.capacity = oneHopResults.getInt("capacity");
	      f.price = oneHopResults.getDouble("price");
	      f.originCity = originCity;
	      f.destCity = destinationCity;
	      
	      flights.add(f);
	      searchedItineraries.add(new Itinerary(f)); // Adds itineraries to list containing the most recent search
	    }
	    oneHopResults.close();
	  return flights;
  }
  
  // Returns a List of an array holding 2 flights. First index is first flight, second is second flight
  private List<Flight[]> twoFlightHop (String originCity, String destinationCity, boolean directFlight, int dayOfMonth, int numberOfItineraries) throws SQLException {

  	flightSearch2Statement.setInt(1, numberOfItineraries);
  	flightSearch2Statement.setString(2, originCity);
  	flightSearch2Statement.setString(3, destinationCity);
  	flightSearch2Statement.setInt(4, dayOfMonth);
  	flightSearch2Statement.setInt(5, dayOfMonth);
  	
  	ResultSet twoHopResults = flightSearch2Statement.executeQuery();
  	
  	// Each element in list is an array of size 2. index 0 is first flight, index 1 is second flight
  	List<Flight[]> twoHopFlights = new ArrayList<Flight[]>();
  	
  	while (twoHopResults.next())
	    {
  			Flight one = new Flight();
  			Flight two = new Flight();
  			
  			one.fid = twoHopResults.getInt("fid1");
  	        one.year = twoHopResults.getInt("year1");
  	        one.monthId = 7;
  	        one.dayOfMonth = dayOfMonth;
  	        one.carrierId = twoHopResults.getString("cid1");
  	        one.flightNum = twoHopResults.getString("flight_num1");
  	        one.time = twoHopResults.getDouble("time1");
  	        one.capacity = twoHopResults.getInt("capacity1");
  	        one.price = twoHopResults.getDouble("price1");
  	        one.originCity = originCity;
  		    one.destCity = twoHopResults.getString("middle_city");
  		    
  		    two.fid = twoHopResults.getInt("fid2");
	        two.year = twoHopResults.getInt("year2");
	        two.monthId = 7;
	        two.dayOfMonth = dayOfMonth;
	        two.carrierId = twoHopResults.getString("cid2");
	        two.flightNum = twoHopResults.getString("flight_num2");
	        two.time = twoHopResults.getDouble("time2");
	        two.capacity = twoHopResults.getInt("capacity2");
	        two.price = twoHopResults.getDouble("price2");
	        two.originCity = twoHopResults.getString("middle_city");
		    two.destCity = destinationCity;
  		
			
			twoHopFlights.add(new Flight[] {one, two});
		    searchedItineraries.add(new Itinerary(one, two)); // Adds itineraries to list containing the most recent search
	    }
	    twoHopResults.close();
	    return twoHopFlights;
  }
    

  /**
   * Implements the book itinerary function.
   *
   * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in the current session.
   *
   * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
   * If try to book an itinerary with invalid ID, then return "No such itinerary {@code itineraryId}\n".
   * If the user already has a reservation on the same day as the one that they are trying to book now, then return
   * "You cannot book two flights in the same day\n".
   * For all other errors, return "Booking failed\n".
   *
   * And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n" where
   * reservationId is a unique number in the reservation system that starts from 1 and increments by 1 each time a
   * successful reservation is made by any user in the system.
   */
  public String transaction_book(int itineraryId)
  {
      if (!loggedIn) return "Cannot book reservations, not logged in\n";
      if (!wasLoggedInWhenSearched) return "Booking failed\n";
	  
      if (itineraryId >= searchedItineraries.size()) return "No such itinerary " + itineraryId + "\n";
      
      Itinerary itin = searchedItineraries.get(itineraryId);
      
      String findCurrId = "SELECT currID FROM ResID";
      
      
	  try {		
		    
		    String getCount1 = "SELECT COUNT(*) AS cnt FROM Reservations WHERE fid1 = ";
		    String getCount2 = "SELECT COUNT(*) AS cnt FROM Reservations WHERE fid2 = ";
		    
		    int fidFlight1 = itin.f1.fid;
		    int capFlight1 = itin.f1.capacity;		    
		    
		    Statement getNumReservations = conn.createStatement();
		    ResultSet numFlight1 = getNumReservations.executeQuery(getCount1 + fidFlight1);
		    
		    numFlight1.next();
		    
		    int numResFlight1 = capFlight1 - numFlight1.getInt("cnt");
		    
		    // This number doesn't matter as long as its above 0. Will be updated if there is a second flight
		    int numResFlight2 = 99999;
		    
		    if (itin.f2 != null) {
		    	int fidFlight2 = itin.f2.fid;
		    	int capFlight2 = itin.f2.capacity;
		    	ResultSet numFlight2 = getNumReservations.executeQuery(getCount2 + fidFlight2);
		    	numFlight2.next();
		    	numResFlight2 = capFlight2 - numFlight2.getInt("cnt");
		    }
		    
		    if (numResFlight1 < 1 || numResFlight2 < 1) return "Booking failed\n";
		    	    
		  
		    if (alreadyHasFlightOnDay(itin)) return "You cannot book two flights in the same day\n";
		  
			Statement searchStatement = conn.createStatement();
			ResultSet currID = searchStatement.executeQuery(findCurrId);
			currID.next();
			
			int nextID = currID.getInt("currID") + 1;
			
			searchStatement.executeUpdate("UPDATE ResID SET  currID = " + nextID);
			
			currID.close();
			
			reservationInsertStatement.setInt(1, nextID);
			reservationInsertStatement.setString(2, username);
			reservationInsertStatement.setInt(3, itin.f1.fid);
			reservationInsertStatement.setInt(4, (itin.f2 == null ? -1 : itin.f2.fid));
			reservationInsertStatement.setInt(5, 0);
			
			reservationInsertStatement.executeUpdate();
		
			return "Booked flight(s), reservation ID: " + (nextID) + "\n";
		
	} catch (SQLException e) {e.printStackTrace();}
	  
	  return "Booking failed\n";
  }
    
  
    // Returns true if person already has a flight on the day they're trying to create a flight reservation for
    // false otherwise.
    // itin is the itinerary they're trying to reserve
    private boolean alreadyHasFlightOnDay(Itinerary itin) {
	    try {
			reservationSameDayCheck.setString(1, username);
			ResultSet checkResult = reservationSameDayCheck.executeQuery();
		    List<Integer> fids = new ArrayList<Integer>();
		    while (checkResult.next()) {
		    	
		    	fids.add(checkResult.getInt("fid1"));
		    	
		    	int posFid2 = checkResult.getInt("fid2");
		    	if (posFid2 != -1) fids.add(posFid2);
		    }
		    
		    checkResult.close();
		    
			if (fids.size() != 0) {
			    String getFlightsOnADay = "SELECT fid FROM Flights WHERE day_of_month = " + itin.f1.dayOfMonth;
			    Statement flightsOfADay = conn.createStatement();
			    ResultSet allFlightsOfADay = flightsOfADay.executeQuery(getFlightsOnADay);
			    
			    while(allFlightsOfADay.next()) {
			    	int fid = allFlightsOfADay.getInt("fid");
			    	if (fids.contains(fid)) return true;
			    }
			    
			    allFlightsOfADay.close();
		    }
			
			return false;
		} catch (SQLException e) {e.printStackTrace();}
	    return false;
    }

  /**
   * Implements the reservations function.
   *
   * @return If no user has logged in, then return "Cannot view reservations, not logged in\n"
   * If the user has no reservations, then return "No reservations found\n"
   * For all other errors, return "Failed to retrieve reservations\n"
   *
   * Otherwise return the reservations in the following format:
   *
   * Reservation [reservation ID] paid: [true or false]:\n"
   * [flight 1 under the reservation]
   * [flight 2 under the reservation]
   * Reservation [reservation ID] paid: [true or false]:\n"
   * [flight 1 under the reservation]
   * [flight 2 under the reservation]
   * ...
   *
   * Each flight should be printed using the same format as in the {@code Flight} class.
   *
   * @see Flight#toString()
   */
  public String transaction_reservations()
  {
	  if (!loggedIn) return "Cannot view reservations, not logged in\n";
	  
	  
	  try {
		  reservationFlightNumsStatement.setString(1, username);
		  ResultSet allFlightNums = reservationFlightNumsStatement.executeQuery();
		  
		  List<Integer[]> fids = new ArrayList<Integer[]>();
		  Map<Integer, Pair> paidMap = new HashMap<Integer, Pair>();
		  
		  while (allFlightNums.next()) {
			  int fid1 = allFlightNums.getInt("fid1");
			  int fid2 = allFlightNums.getInt("fid2");
			  fids.add(new Integer[]{fid1, fid2});
			  
			  paidMap.put(fid1, new Pair(allFlightNums.getInt("id"), allFlightNums.getBoolean("paid")));
		  }
		  
		  allFlightNums.close();
		  
		  List<Itinerary> itins = setItinerariesFromFids(fids);
		  
		  return convertItineraryToString(itins, paidMap);
	} catch (SQLException e) {e.printStackTrace();}
	  
      return "Failed to retrieve reservations\n";
  }
  
  private List<Itinerary> setItinerariesFromFids (List<Integer[]> fids) {
	  List<Itinerary> result = new ArrayList<Itinerary>();
	  String getFlightInfo = "SELECT year, month_Id, day_of_month, carrier_id, flight_num, origin_city, dest_city, actual_time, capacity, price "
	  		+ "FROM Flights WHERE fid = ";
	  
	  try {
		  Statement flightInfo = conn.createStatement();
		  for (int i = 0; i < fids.size(); i++) {
			  // This means we only have 1 flight for the itinerary
			  Flight f1 = new Flight();
			  int fid1 = fids.get(i)[0];
			  
			  ResultSet flightSet = flightInfo.executeQuery(getFlightInfo + fid1);
			  flightSet.next();
			  
			  f1.fid = fid1;
			  f1.year = flightSet.getInt("year");
			  f1.monthId = flightSet.getInt("month_Id");
			  f1.dayOfMonth = flightSet.getInt("day_of_month");
			  f1.carrierId = flightSet.getString("carrier_id");
			  f1.flightNum = flightSet.getString("flight_num");
			  f1.originCity = flightSet.getString("origin_city");
			  f1.destCity = flightSet.getString("dest_city");
			  f1.time = flightSet.getInt("actual_time");
			  f1.capacity = flightSet.getInt("capacity");
			  f1.price = flightSet.getDouble("price");
			  
			  if (fids.get(i)[1] != -1) {
				  int fid2 = fids.get(i)[1];
				  Flight f2 = new Flight();
				  ResultSet flightSet2 = flightInfo.executeQuery(getFlightInfo + fid2);
				  f2.fid = fid1;
				  f2.year = flightSet2.getInt("year");
				  f2.monthId = flightSet2.getInt("month_Id");
				  f2.dayOfMonth = flightSet2.getInt("day_of_month");
				  f2.carrierId = flightSet2.getString("carrier_id");
				  f2.flightNum = flightSet2.getString("flight_num");
				  f2.originCity = flightSet2.getString("origin_city");
				  f2.destCity = flightSet2.getString("dest_city");
				  f2.time = flightSet2.getInt("actual_time");
				  f2.capacity = flightSet2.getInt("capacity");
				  f2.price = flightSet2.getDouble("price");
				  result.add(new Itinerary(f1, f2));
			  } else {
				  result.add(new Itinerary(f1, null));
			  }
			  flightSet.close();
		  }
	} catch (SQLException e) {e.printStackTrace();}

	  return result;
  }
  
  //* Reservation [reservation ID] paid: [true or false]:\n"
  //* [flight 1 under the reservation]
  //* [flight 2 under the reservation]
  
  
  
  
  private String convertItineraryToString(List<Itinerary> itins, Map<Integer, Pair> paidMap) {
	  
	  if (itins.size() == 0) return "No reservations found\n";
	  
	  StringBuffer sb = new StringBuffer();
	  
	  for (int i = 0; i < itins.size(); i++) {
		  Itinerary it = itins.get(i);
		  Pair currPair = paidMap.get(it.f1.fid);
		  if (it.f2 == null) { // only 1 flight
			  sb.append("Reservation " + currPair.id + " paid: " + currPair.paid + ":\n");
			  
			  sb.append("ID: " + it.f1.fid + " Date: " + it.f1.year + "-" + it.f1.monthId + "-" + it.f1.dayOfMonth + " Carrier: " + 
				         it.f1.carrierId + " Number: " + it.f1.flightNum + " Origin: " + it.f1.originCity + " Dest: " + 
				         it.f1.destCity + " Duration: " + it.f1.time + " Capacity: " + it.f1.capacity + " Price: " + it.f1.price + "\n");
		  } else {
			  sb.append("Reservation " + currPair.id + " paid: " + currPair.paid + ":\n");
			  
			  sb.append("ID: " + it.f2.fid + " Date: " + it.f2.year + "-" + it.f2.monthId + "-" + it.f2.dayOfMonth + " Carrier: " + 
				         it.f2.carrierId + " Number: " + it.f2.flightNum + " Origin: " + it.f2.originCity + " Dest: " + 
				         it.f2.destCity + " Duration: " + it.f2.time + " Capacity: " + it.f2.capacity + " Price: " + it.f2.price + "\n");
		  }
	  }
	  
	  return sb.toString();
  }

  /**
   * Implements the cancel operation.
   *
   * @param reservationId the reservation ID to cancel
   *
   * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n"
   * For all other errors, return "Failed to cancel reservation [reservationId]"
   *
   * If successful, return "Canceled reservation [reservationId]"
   *
   * Even though a reservation has been canceled, its ID should not be reused by the system.
   */
  public String transaction_cancel(int reservationId)
  {
	  if (!loggedIn) return "Cannot cancel reservations, not logged in\n";
	  
	  try {
		reservationExists.setString(1, username);
		reservationExists.setInt(2, reservationId);
		
		
		ResultSet theReservation = reservationExists.executeQuery();
		
		
		if (!theReservation.next()) return "Failed to cancel reservation " + reservationId + "\n";
		
		boolean paid = theReservation.getBoolean("paid");
		int fid1 = theReservation.getInt("fid1");
		int fid2 = theReservation.getInt("fid2");
		
		if (paid) { // This section refunds money to user
			flightPrice.setInt(1, fid1);
			ResultSet price1 = flightPrice.executeQuery();
			price1.next();
			double refundAmt1 = price1.getDouble("price");
			
			flightPrice.setInt(1, fid2);
			ResultSet price2 = flightPrice.executeQuery();
			
			// Possibly no second flight, if there is none then there is nothing to refund
			double refundAmt2 = 0;
			if(price2.next()) 
				refundAmt2 = price2.getDouble("price");
			
			price1.close();
			price2.close();
			
			double refundTotal = refundAmt1 + refundAmt2;
			
			refundStatement.setDouble(1, refundTotal);
			refundStatement.setString(2, username);
			refundStatement.executeUpdate();
		} 
		
		deleteReservationStatement.setInt(1, reservationId);
		deleteReservationStatement.executeUpdate();
		
		return "Canceled reservation " + reservationId + "\n";
		
	} catch (SQLException e) {e.printStackTrace();}
	  
	  
	  return "Failed to cancel reservation " + reservationId + "\n";
  }

  /**
   * Implements the pay function.
   *
   * @param reservationId the reservation to pay for.
   *
   * @return If no user has logged in, then return "Cannot pay, not logged in\n"
   * If the reservation is not found / not under the logged in user's name, then return
   * "Cannot find unpaid reservation [reservationId] under user: [username]\n"
   * If the user does not have enough money in their account, then return
   * "User has only [balance] in account but itinerary costs [cost]\n"
   * For all other errors, return "Failed to pay for reservation [reservationId]\n"
   *
   * If successful, return "Paid reservation: [reservationId] remaining balance: [balance]\n"
   * where [balance] is the remaining balance in the user's account.
   */
  public String transaction_pay (int reservationId)
  {
	  
	  if (!loggedIn) return "Cannot pay, not logged in\n";
	  
	  try {
		  reservationExists.setString(1, username);
		  reservationExists.setInt(2, reservationId);
		  ResultSet possibleReservation = reservationExists.executeQuery();
		  
		  if (!possibleReservation.next()) return "Cannot find unpaid reservation " + reservationId + " under user: " + username + "\n";
		  
		  boolean paid = possibleReservation.getBoolean("paid");
		  int fid1 = possibleReservation.getInt("fid1");
		  int fid2 = possibleReservation.getInt("fid2");
		  
		  
		  if (!paid) { // This section refunds money to user
				flightPrice.setInt(1, fid1);
				ResultSet price1 = flightPrice.executeQuery();
				price1.next();
				double cost1 = price1.getDouble("price");
				
				flightPrice.setInt(1, fid2);
				ResultSet price2 = flightPrice.executeQuery();
				
				// Possibly no second flight, if there is none then there is nothing to refund
				double cost2 = 0;
				if(price2.next()) 
					cost2 = price2.getDouble("price");
				
				price1.close();
				price2.close();
				
				double costTotal = cost1 + cost2;
				
				getBalanceStatement.setString(1, username);
				ResultSet userBalance = getBalanceStatement.executeQuery();
				userBalance.next();
				
				double balance = userBalance.getDouble("balance");
				
				userBalance.close();
				
				if (balance < costTotal) return "User has only " + String.format(Locale.US, "%.2f", balance) + " in account but itinerary costs " + costTotal + "\n";
				
				double remainingBalance = balance - costTotal;
				
				payStatement.setDouble(1, remainingBalance);
				payStatement.setString(2, username);
				payStatement.executeUpdate();
				
				String updateAsPaid = "UPDATE Reservations SET paid = 1 WHERE id = " + reservationId;
				Statement updatePayStatement = conn.createStatement();
				
				updatePayStatement.executeUpdate(updateAsPaid);
				
				return "Paid reservation: " + reservationId + " remaining balance: " + remainingBalance + "\n";
		  }
		  
	} catch (SQLException e) {e.printStackTrace();}
	  
      return "Failed to pay for reservation " + reservationId + "\n";
  }

  /* some utility functions below */

  public void beginTransaction() throws SQLException
  {
    conn.setAutoCommit(false);
    beginTransactionStatement.executeUpdate();
  }

  public void commitTransaction() throws SQLException
  {
    commitTransactionStatement.executeUpdate();
    conn.setAutoCommit(true);
  }

  public void rollbackTransaction() throws SQLException
  {
    rollbackTransactionStatement.executeUpdate();
    conn.setAutoCommit(true);
  }

  /**
   * Shows an example of using PreparedStatements after setting arguments. You don't need to
   * use this method if you don't want to.
   */
  private int checkFlightCapacity(int fid) throws SQLException
  {
    checkFlightCapacityStatement.clearParameters();
    checkFlightCapacityStatement.setInt(1, fid);
    ResultSet results = checkFlightCapacityStatement.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }
}
