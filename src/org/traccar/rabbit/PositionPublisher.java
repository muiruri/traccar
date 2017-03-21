package org.traccar.rabbit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.traccar.model.Position;

import javax.json.Json;
import javax.json.JsonObjectBuilder;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by kenny on 18/03/2017.
 */
public class PositionPublisher {

    private static final String EXCHANGE_NAME = "positions";

    public void publishPosition(Position position) {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            JsonObjectBuilder j = Json.createObjectBuilder();
            j.add("lon", position.getLongitude());
            j.add("lat", position.getLatitude());
            j.add("deviceId", position.getDeviceId());
            String message = j.build().toString();

            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");

            channel.close();
            connection.close();
        } catch (IOException io) {
            System.out.println("IOException");
            io.printStackTrace();
        } catch (TimeoutException toe) {
            System.out.println("TimeoutException : " + toe.getMessage());
            toe.printStackTrace();
        }
    }
}
