import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executors;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;

import java.nio.file.Paths;

import static java.lang.Thread.sleep;
import static java.time.LocalDateTime.*;

public class rentalStream {

    //    private String creds = "/Users/matthew.miller/Downloads/secure-connect-mdse.zip";
//    String username = "mmiller";
//    String password = "cassandra1!";
    static String keyspace = "keyspace1";
    static DseSession session = DseSession.builder()
            .withCloudSecureConnectBundle(Paths.get("/Users/matthew.miller/Downloads/secure-connect-mdse1.zip"))
            .withAuthCredentials("mmiller", "cassandra1!")
            .withKeyspace(keyspace)
            .build();


    public static void main(String[] args) throws InterruptedException {
        // Calls rental stream then exits once complete
        //    Create connection to Apollo
        new rentalStream();
//
//    }
//    public rentalStream() throws InterruptedException {

        Random rand = new Random();
        int count = 0;
        int number_of_rentals = 3; //define the number of rentals you want to generate
        while (count < number_of_rentals) {
            UUID rental_id = UUID.randomUUID();
            String rental_start = now().toString();
            double start_fuel_level = 100;
            double end_fuel_level;
            String start_location = getStartLocation();
            String end_location = getEndLocation(start_location);
            String vehicle_id = getVehicleDetails().get(0);
            String account_email = getAccountEmail();

            int rental_duration = rand.nextInt(60 - 15); //Rentals will last at least 15 seconds but no more than 60 seconds
            int i = 0;
            int event_frequency_ms = 1000;
            double current_fuel_level = start_fuel_level;

            // While trip is happening, write the data to Apollo table 'live_trip'
            System.out.println("New Rental");
            System.out.println("-----------");
            while (i < rental_duration){
                String current_time = LocalDateTime.now().toString();
                double speed = generateSpeed();
                current_fuel_level = generateFuelLevel(current_fuel_level);
                System.out.println("Speed = "+speed+
                        ", Fuel = " +current_fuel_level+
                        ", Vehicle No: "+vehicle_id+
                        ", Time : "+ current_time+
                        ", Driver: " +account_email);
                String current_query = "INSERT INTO keyspace1.live_trip (speed, timestamp, vehicle_id, trip_id) VALUES(?, ?, ?, ?)";
                PreparedStatement current_trip_event = session.prepare(current_query);

                BoundStatement bound = current_trip_event.bind(speed, current_time, vehicle_id, rental_id);
                session.execute(bound);

                sleep(event_frequency_ms);
                i++;
            }
            //Once trip is done, complete the 'rental' record and write it to apollo
            end_fuel_level = current_fuel_level;
            String rental_stop = now().toString();
            double miles_driven = i * 10.125;

//            String sfuel = String.valueOf(start_fuel_level);
//            String efuel = String.valueOf(end_fuel_level);
            // CQL statement that inserts aggregate data into the

            String insert_trip_details = "INSERT INTO keyspace1.rentals (" +
                    "rental_id, " +
                    "vehicle_id, " +
                    "rental_start, " +
                    "rental_stop, " +
                    "start_fuel, " +
                    "end_fuel, " +
                    "start_location, " +
                    "end_location, " +
                    "miles_driven, " +
                    "account_email) VALUES ("+
                    ""+ rental_id+", " +
                    "'"+ vehicle_id+"', " +
                    "'"+ rental_start+"', " +
                    "'"+ rental_stop+"', " +
                    ""+ start_fuel_level+", " +
                    ""+ end_fuel_level+", " +
                    "'"+ start_location+"', " +
                    "'"+ end_location+"', " +
                    ""+ miles_driven+", " +
                    "'"+ account_email+"');";
            // **********THIS NEEDS TO BE CHANGED TO A PREPARED STATEMENT****************
            SimpleStatement trip_insert = SimpleStatement.builder(insert_trip_details)
                    .setConsistencyLevel(ConsistencyLevel.EACH_QUORUM)
                    .setKeyspace(keyspace)
                    .build();
            session.execute(trip_insert);
            //***************************************************************************

            // Print out the same info that was just written to DSE
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

        System.exit(0);
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

        String id = clean_vehicle_ids.get(rand.nextInt(raw_vehicle_ids.size()));

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

        String city_query = "SELECT airport_code FROM rental_locations;";
        SimpleStatement city_statement = SimpleStatement.builder(city_query)
                .setConsistencyLevel(ConsistencyLevel.EACH_QUORUM)
                .setKeyspace(keyspace)
                .build();
        ResultSet rs = session.execute(city_statement);
        List<Row> raw_airports = rs.all();
        List<String> clean_airports = new ArrayList<String>();
        int count = 0;
        while (count < raw_airports.size()){
            clean_airports.add(raw_airports.get(count).getString("airport_code"));
            count++;
        }

        String airport_code = clean_airports.get(rand.nextInt(raw_airports.size()));

        String data_query = "SELECT * FROM  rental_locations where airport_code ='" + airport_code + "';";
        SimpleStatement statement = SimpleStatement.builder(data_query)
                .setConsistencyLevel(ConsistencyLevel.EACH_QUORUM)
                .setKeyspace(keyspace)
                .build();
        rs = session.execute(statement);
        Row row = rs.one();

        //Get each of the fields within the row
        return row.getString("airport_code");
    }

    private static String getEndLocation(String start_location) {
        Random rand = new Random();
        String city_query = "SELECT airport_code FROM rental_locations;";
        SimpleStatement city_statement = SimpleStatement.builder(city_query)
                .setConsistencyLevel(ConsistencyLevel.EACH_QUORUM)
                .setKeyspace(keyspace)
                .build();
        ResultSet rs = session.execute(city_statement);
        List<Row> raw_airports = rs.all();
        List<String> clean_airports = new ArrayList<String>();
        int count = 0;
        while (count < raw_airports.size()){
            clean_airports.add(raw_airports.get(count).getString("airport_code"));
            count++;
        }

        // 20% chance of returning the car to a different airport
        int chance = rand.nextInt(10);
        String end_location = null;
        if(chance > 8){
            end_location = clean_airports.get(rand.nextInt(clean_airports.size()));
        }
        else{
            end_location = start_location;
        }
        return end_location;
    }

    private static String getAccountEmail() {
        Random rand = new Random();
        String city_query = "SELECT email FROM accounts;";
        SimpleStatement city_statement = SimpleStatement.builder(city_query)
                .setConsistencyLevel(ConsistencyLevel.EACH_QUORUM)
                .setKeyspace(keyspace)
                .build();
        ResultSet rs = session.execute(city_statement);
        List<Row> raw_emails = rs.all();
        List<String> clean_emails = new ArrayList<String>();
        int count = 0;
        while (count < raw_emails.size()){
            clean_emails.add(raw_emails.get(count).getString("email"));
            count++;
        }
        String email = clean_emails.get(rand.nextInt(raw_emails.size()));
        return email;
    }
}
