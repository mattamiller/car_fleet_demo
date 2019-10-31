import java.time.LocalDateTime;
import java.util.*;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import static java.lang.Thread.sleep;

public class rentalStream {
    public static void main (String[] args) throws InterruptedException {
        // Apollo connection details
        String creds = "/Users/matthew.miller/Documents/secure-connect-mattdb.zip";
        String username = "mmiller";
        String password = "cassandra";
        String keyspace = "keyspace1";

        // start trip from pickup location, start fuel level, duration of rental
        Random rand = new Random();
        int count = 0; //count for first while loop
        int number_of_rentals = 3;

        while (count < number_of_rentals) {
            System.out.println("New Rental");
            System.out.println("-----------");
            String rental_id = UUID.randomUUID().toString();
            String rental_start = LocalDateTime.now().toString();
            double start_fuel_level = 100;
            double end_fuel_level;
            String start_location = getStartLocation();
            String end_location = getEndLocation(start_location);
            String vehicle_id = getVehicleDetails().get(0);
            String account_email = getAccountEmail();

//            int duration = 5;
            int rental_duration = rand.nextInt(60 - 15); //Rentals will last at least 15 seconds but no more than 60 seconds
            int i = 0;
            int event_frequency_ms = 1000;
            double current_fuel_level = start_fuel_level;

            // While trip is happening, write the data to Apollo table 'live_trip'
            while (i < rental_duration){
                String time = LocalDateTime.now().toString();
                double speed = generateSpeed();
                current_fuel_level = generateFuelLevel(current_fuel_level);
                System.out.println("Speed = "+speed+
                        ", Fuel = " +current_fuel_level+
                        ", Vehicle No: "+vehicle_id+
                        ", Time : "+time+
                        ", Driver: " +account_email);
                sleep(event_frequency_ms);
                i++;
            }

            //Once trip is done, complete the 'rental' record and write it to apollo
            end_fuel_level = current_fuel_level;
            String rental_stop = LocalDateTime.now().toString();
            double miles_driven = i * 10.125;

            System.out.println();
            System.out.println("End of rental.  See below for Rental Data:");
            System.out.println(
                            "Rental ID: " +rental_id +"\n"+
                                    "Rental Start: " +rental_start +"\n"+
                                    "Rental Stop: " +rental_stop +"\n"+
                                    "Vehicle ID: " +vehicle_id +"\n"+
                                    "Starting Fuel Level: " +start_fuel_level +"\n"+
                                    "Ending Fuel Level: " +end_fuel_level +"\n"+
                                    "Start Location: " +start_location +"\n"+
                                    "End Location: " +end_location +"\n"+
                                    "Miles Driven :" +miles_driven +"\n"+
                                    "Account: " +account_email
            );
            System.out.println("--------------------------------------------");
            System.out.println();
            System.out.println();
            count ++;
        }
    }

    private static DseSession clusterConnect(String creds, String username, String password, String keyspace) {
        // Connect to DSE Apollo
        DseSession session = null;
        try {
            session = DseSession.builder()
                    .withCloudSecureConnectBundle(creds)
                    .withAuthCredentials(username, password)
                    .withKeyspace(keyspace)
                    .build();

        } catch (Exception e) {
            System.out.println("Error occurred opening the session. " + e.getMessage());
        }
        ResultSet rs = session.execute("select release_version from system.local");
        Row row = rs.one();
        //Print the results of the CQL query to the console:
        if (row != null) {
            System.out.println(row.getString("release_version"));
        } else {
            System.out.println("An error occurred.");
        }
        return session;
    }

    // Method to generate speed by itself
    private static Double generateSpeed() {
        Random rand = new Random();
        double speed = rand.nextInt(65 - 25) + 20;// random speed between 25 and 60
        return speed;
    }

    // Method to generate Fuel Level by itself
    private static Double generateFuelLevel(Double current_fuel_level) {
        double new_fuelLevel = current_fuel_level - 1.75;
        return new_fuelLevel;
    }

    // Method hits Apollo and returns vehicle details (at random) from the keyspace1.vehicles table
    private static List<String> getVehicleDetails() {
        String creds = "/Users/matthew.miller/Documents/secure-connect-mattdb.zip";
        String username = "mmiller";
        String password = "cassandra";
        String keyspace = "keyspace1";
        DseSession session = clusterConnect(creds, username, password, keyspace);
        // code here
        //  Randomly selects a vehicle from the inventory, then looks up the details for that vehicle
        //  That vehicle is then rented out
        Random rand = new Random();
        String id_query = "SELECT id FROM vehicles;";
        SimpleStatement id_statement = SimpleStatement.builder(id_query)
                .setConsistencyLevel(ConsistencyLevel.EACH_QUORUM)
                .setKeyspace(keyspace)
                .build();
        ResultSet rs = session.execute(id_statement);
        List<Row> raw_vehicle_ids = rs.all();
        List<String> clean_vehicle_ids = new ArrayList<String>();
        int count = 0;
        while (count < raw_vehicle_ids.size()){
            clean_vehicle_ids.add(raw_vehicle_ids.get(count).getString("id"));
            count++;
        }

        String id = clean_vehicle_ids.get(rand.nextInt(7));

        // Connect to Apollo and query for the vehicle ID that was randomly picked
        String data_query = "SELECT * FROM  vehicles where id ='" + id + "';";
        SimpleStatement statement = SimpleStatement.builder(data_query)
                .setConsistencyLevel(ConsistencyLevel.EACH_QUORUM)
                .setKeyspace(keyspace)
                .build();
        rs = session.execute(statement);
        Row row = rs.one();

        //Get each of the fields within the row
        String vehicle_id = row.getString("id");
        String make = row.getString("make");
        String model = row.getString("model");
        String year = row.getString("year");
        String type = row.getString("type");

        // Return the results in the form of a String List
        List<String> vehicle_details = Arrays.asList(vehicle_id, make, model, year, type);
        return vehicle_details;
    }

    private static String getStartLocation(){
        Random rand = new Random();
        List<String> airport_list = Arrays.asList("SJC",
                "SFO",
                "San Francisco",
                "San Jose",
                "Mountain View",
                "Cupertino",
                "Palo Alto",
                "Santa Clara",
                "Santa Cruz",
                "Los Gatos");
        String airport_code = airport_list.get(rand.nextInt(10));
        return airport_code;
    }

    private static String getEndLocation(String start_location) {
        Random rand = new Random();
        List<String> airport_list = Arrays.asList("SJC",
                "SFO",
                "San Francisco",
                "San Jose",
                "Mountain View",
                "Cupertino",
                "Palo Alto",
                "Santa Clara",
                "Santa Cruz",
                "Los Gatos");
        int chance = rand.nextInt(10);
        String end_location = null;
        // 10% chance of returning the car to a different airport
        if(chance > 9){
            end_location = airport_list.get(rand.nextInt(10));
        }
        else{
            end_location = start_location;
        }
        return end_location;
    }

//    private static List<String> getAccountDetails(String creds){
//        String username = "mmiller";
//        String password = "cassandra";
//        String keyspace = "keyspace1";
//
//    }
    private static String getAccountEmail() {
        Random rand = new Random();
        List<String> account_list = Arrays.asList("matt@testemail.org'",
                "foo@aol.com",
                "sam@gmail.com'",
                "ft@hotmail.com",
                "jpn@yahoo.com",
                "qanders@gmail.com",
                "abogdon@outlook.com",
                "smithers@gmail.com",
                "kimmc@yahoo.com",
                "sj@outlook.com");
        String email = account_list.get(rand.nextInt(7));
        return email;
    }
}
