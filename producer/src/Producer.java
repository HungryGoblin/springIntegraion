import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

public class Producer {
    public static final String EXCHANGE_NAME = "MESSAGE_QUEUE";
    private final ConnectionFactory factory;
    private final Connection connection;
    private final Channel channel;

    public void publish(String messageType, String message) throws IOException {
        channel.basicPublish(EXCHANGE_NAME, messageType,null, message.getBytes());
        System.out.printf("MESSAGE PUBLISHED [%s]: %s%n", messageType, message);
    }

    public Producer() throws IOException, TimeoutException {
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(EXCHANGE_NAME, false, false, false, null);
    }

    public static void main(String[] args) {
        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(System.in))) {
            String[] messArray;
            Producer producer = new Producer();
            while (true) {
                System.out.print("ENTER MESSAGE (TYPE MESSAGE): ");
                String message = reader.readLine();
                messArray = message.split(" ");
                    producer.publish(messArray[0], message.substring(messArray[0].length() + 1));
            }
        } catch (IOException | TimeoutException e) {
            System.out.printf("An error has occurred: %s%n", e.getMessage());
            e.printStackTrace();
        }
    }
}
