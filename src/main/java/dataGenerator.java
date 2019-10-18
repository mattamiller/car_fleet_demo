import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import static java.lang.Thread.sleep;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;


import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;


//Author: Matt Miller

public class dataGenerator {
    public static void main(String[] args) throws InterruptedException {
        String vehicleId = null;
        String vehicleType = null;
        String routeId = null;
        double speed;
        double fuelLevel;

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        String now = LocalDateTime.now().toString();

        // Define DSE connection & CQL Statements
        DseSession session = clusterConnect("/Users/matthew.miller/cassandra/secure-connect-mdb1.zip", "mmiller", "cassandra");


        // Define number of events that will be generated and how often you want to events to generate
        int i = 0;
        int num_events = 10;
        int event_frequency_ms = 1000;
        while (i < num_events) {
            vehicleId = generateVehicleId();
            vehicleType = generateVehicleType();
            routeId = generateRouteId();
            speed = generateSpeed();
            fuelLevel = generateFuelLevel();

            System.out.println(generateEvent());
            System.out.println(
                    "VehicleID: " + vehicleId
                            + "\nVehicle Type: " + vehicleType
                            + "\nRoute: " + routeId
                            + "\nSpeed: " + speed
                            + "\nFuel Level: " + fuelLevel
                            + "\nTimeStamp: " + now
                            + "\n\n-------------------"
            );

            String query = "INSERT INTO keyspace1.Total_Traffic (routeid, vehicleType, timestamp, recordDate) " +
                    "VALUES ('"+routeId+"','"+vehicleType+"','"+now+"','"+now+"')";
            SimpleStatement statement = SimpleStatement.builder(query)
                    .setConsistencyLevel(ConsistencyLevel.EACH_QUORUM)
                    .build();
            session.execute(statement);

            i++;
            sleep(event_frequency_ms);
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
    //Single line, comma separated "iot event"
    private static String generateEvent() {
        List<String> routeList = Arrays.asList(new String[]{"Route-37", "Route-43", "Route-82", "Road-15"});
        List<String> vehicleTypeList = Arrays.asList(new String[]{"Large Truck", "Small Truck", "Large SUV", "Small SUV", "Van", "Compact", "Sedan"});
        Random rand = new Random();

        String vehicleId = UUID.randomUUID().toString();
        String vehicleType = vehicleTypeList.get(rand.nextInt(7));
        String routeId = routeList.get(rand.nextInt(4));
        double speed = rand.nextInt(100 - 20) + 20;// random speed between 20 to 100
        double fuelLevel = rand.nextInt(40 - 10) + 10;

        String event = vehicleId + ',' + vehicleType + ',' + routeId + ',' + speed + ',' + fuelLevel;
        return event;

    }
    // Method to generate VehicleID by itself
    private static String generateVehicleId() {
        String vehicleId = UUID.randomUUID().toString();
        return vehicleId;
    }
    // Method to generate Vehicle Type by itself
    private static String generateVehicleType() {
        Random rand = new Random();
        List<String> vehicleTypeList = Arrays.asList(new String[]{"Large Truck", "Small Truck", "Large SUV", "Small SUV", "Van", "Compact", "Sedan"});
        String vehicleType = vehicleTypeList.get(rand.nextInt(7));
        return vehicleType;
    }

    // Method to generate Route by itself
    private static String generateRouteId() {
        Random rand = new Random();
        List<String> routeList = Arrays.asList(new String[]{"Route-37", "Route-43", "Route-82",  "Road-15"});
        String routeId = routeList.get(rand.nextInt(4));
        return routeId;
    }

    // Method to generate speed by itself
    private static Double generateSpeed() {
        Random rand = new Random();
        double speed = rand.nextInt(100 - 20) + 20;// random speed between 20 to 100
        return speed;
    }

    // Method to generate Fuel Level by itself
    private static Double generateFuelLevel() {
        Random rand = new Random();
        double fuelLevel = rand.nextInt(40 - 10) + 10;
        return fuelLevel;
    }


}
