import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Hello world!
 *
 */
public class KafkaSparkApp {

    public static void main( String[] args ) {
        //Отключение логов Kafka и Spark
        Logger.getLogger("org").setLevel(Level.OFF);

        KafkaProd producer = new KafkaProd();

        SparkConsumer consumer = new SparkConsumer();
        //Выведение Spark Consumer в отдельный поток
        Thread consThread = new Thread(consumer);
        consThread.start();
        //Создание сканера для ввода сообщений
        Scanner in = new Scanner(System.in);
        String message =in.nextLine();
        //Цикл считывания и отправки сообщений брокеру Kafka, пока пользователь не введет команду выхода
        while(!message.equals("q")) {
            producer.sendMessage(message);
            message = in.nextLine();
        }

        in.close();
        producer.close();
        consumer.stop();
    }
}
