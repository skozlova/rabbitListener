import com.rabbitmq.client.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

public class Receiver {

    private final static String[] QUEUES = {"cyber_conversion_session_success", "cyber_conversion_session_error",
            "cyber.media-processor-watermark", "cyber_conversion_quality_success", "cyber_conversion_thumbnail_success"};
    private final static String HOST = "localhost";
    private final static int PORT = 5672;
    private final static String USER = "loadtest";
    private final static String PASSWORD = "loadtest";


    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(PORT);
        factory.setUsername(USER);
        factory.setPassword(PASSWORD);
        factory.setVirtualHost("/");

        Connection connection = factory.newConnection();

        Channel channel = connection.createChannel();

        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date());
        String rabbitLog = "rabbit_" + timestamp + ".csv";
        try {
            Files.deleteIfExists(Path.of(rabbitLog));
            Files.write(Path.of(rabbitLog), Collections.singleton("timestamp session_id video_id"));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        BufferedWriter writerRabbit = new BufferedWriter(new FileWriter(rabbitLog, true));
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.ms");

        DeliverCallback deliverCallback = (s, delivery) -> {
            String jsonString = new String(delivery.getBody(), "UTF-8");
            JSONObject jsonObject = null;
            String session_id = "";
            String video_id = "";
            try {
                JSONArray jsonArray = new JSONArray(jsonString);
                jsonObject = new JSONObject(jsonArray.getJSONArray(0).get(0).toString());
                session_id = jsonObject.getString("session_id");
                video_id = jsonObject.getString("video_id");
            } catch (JSONException e) {
                e.printStackTrace();
            }

            writerRabbit.append(dtf.format(LocalDateTime.now()) + " " + session_id + " " + video_id + "\n");

            System.out.println("\n" + dtf.format(LocalDateTime.now()));
            System.out.println(new String(delivery.getBody(), "UTF-8"));
            System.out.println(delivery.getEnvelope());
            System.out.println(delivery.getProperties().getTimestamp());

        };

        CancelCallback cancelCallback = s -> {
            System.out.println(s);
        };
        channel.basicConsume(QUEUES[0], true, deliverCallback, cancelCallback);
    }
}
