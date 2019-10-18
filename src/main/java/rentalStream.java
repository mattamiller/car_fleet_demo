import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.UUID;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import static java.lang.Thread.sleep;

public class rentalStream {
    public static void main (String[] args) throws InterruptedException {
        // start trip from pickup location, start fuel level, duration of rental
        Random rand = new Random();
        int count = 0; //count for first while loop
        int number_of_rentals = 3;

        while (count < number_of_rentals) {
            String rental_id = UUID.randomUUID().toString();
            String rental_start = LocalDateTime.now().toString();
            double start_fuel_level = 100;
            double end_fuel_level;
            String start_location = getStartLocation();
            String end_location = getEndLocation(start_location);
            String vehicle_id = getVehicle();
            String account_email = getAccountEmail();

            int duration = 5;
//            int duration = rand.nextInt(10 - 5); //Rentals will last at least 15 seconds but no more than 60 seconds
            int i = 0;
            int event_frequency_ms = 1000;
            double current_fuel_level = start_fuel_level;

            // While trip is happening, write the data to Apollo table 'live_trip'

            while (i < duration){
                double speed = generateSpeed();
                current_fuel_level = generateFuelLevel(current_fuel_level);
                System.out.println("Speed = "+speed+", Fuel = " +current_fuel_level+", Vehicle No: "+vehicle_id);
                sleep(event_frequency_ms);
                i++;
            }

            //Once trip is done, complete the 'rental' record and write it to apollo
            end_location = getEndLocation(start_location);
            end_fuel_level = current_fuel_level;
            String vehicle_type = getVehicleType();
            String rental_stop = LocalDateTime.now().toString();


            System.out.println("End of rental.  See below for Rental Data:");
            System.out.println(
                    rental_id +','+
                    rental_start +','+
                    rental_stop +','+
                    vehicle_id +','+
                    vehicle_type+','+
                    start_fuel_level +','+
                    end_fuel_level +','+
                    start_location +','+
                    end_location +','+
                    account_email
            );
            count ++;
        }
    }

    private static DseSession clusterConnect(String creds, String username, String password) {
        // Connect to DSE Apollo
        DseSession session = null;
        try {
            session = DseSession.builder()
                    .withCloudSecureConnectBundle(creds)
                    .withAuthCredentials("mmiller", "cassandra")
                    .withKeyspace("keyspace1")
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

    private static String getVehicle() {
        String vehicle_id = "ct2019ls-0001";
        // code here
        return vehicle_id;
    }

    private static String getStartLocation(){
        String location = "SJC";
        return location;
    }

    private static String getEndLocation(String start_location) {
        return start_location;
    }

    private static String getAccountEmail() {
        String email = "foo@aol.com";
        return email;
    }

    private static String getVehicleType() {
        String vehicle_type = "Large SUV";
        return vehicle_type;
    }
}
