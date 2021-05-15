import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;

public class Consumer {
    public static final String EXCHANGE_NAME = "MESSAGE_QUEUE";
    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    private String queueName;
    private String messageType;

    public Consumer() throws IOException, TimeoutException {
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        queueName = channel.queueDeclare().getQueue();
    }

    public void bindQueue(String messageType) throws IOException {
        channel.queueBind(queueName, EXCHANGE_NAME, messageType);
        this.messageType = messageType;
    }


    public static void main(String[] args) {
        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(System.in))) {
            String[] commandArray;
            String command;
            Consumer consumer = new Consumer();
            System.out.print("ENTER COMMAND:");
            command = reader.readLine();
            commandArray = command.split(" ");
            if (commandArray[0].equals("set_topic")) {
                consumer.bindQueue(commandArray[1]);
                System.out.printf("QUEUE NAME: %s%n", consumer.queueName);
                System.out.printf("MESSAGE TYPE: %s%n", consumer.messageType);
                System.out.println("LISTENING... (CTRL-C to break)");
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.printf("MESSAGE RECEIVED [%s]: %s%n", delivery.getEnvelope().getRoutingKey(), message);
                    System.out.println("LISTENING... (CTRL-C to break)");
                };
                consumer.channel.basicConsume(consumer.queueName, true, deliverCallback, consumerTag -> { });
            }
            else {
                System.out.printf("WRONG COMMAND: %s%n", command);
                System.exit(1);
            }
        } catch (IOException | TimeoutException e) {
            System.out.printf("An error has occurred: %s%n", e.getMessage());
            e.printStackTrace();
        }
    }
}
