import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

//Класс-оболочка для Spark Consumer
public class SparkConsumer implements Runnable{

    Connection connection = null; //объект для соединения с бд
    Statement statement; //объект для доступа к таблицам и генерации SQL-команд
    JavaStreamingContext jssc = null; //Функциональый объект Spark, необходим для создания объектов типа DStream

    @SuppressWarnings("serial")
    @Override
    public void run(){
        try {
            //Установка соединения с бд
            connection = DriverManager.getConnection(
                    "jdbc:postgresql://127.0.0.1:5432/postgres", //Адрес базы данных
                    "postgres", //Имя пользователя
                    ""); //Пароль
            //Создание утверждения для доступа к таблицам базы данных
            statement = connection.createStatement();
            System.out.println("Успешное подключение к базе данных postgres");
        } catch (SQLException e) {

            System.out.println("Не удалось подключиться к базе данных");
            System.exit(-1);
        }
        System.out.println("Создание Spark Consumer для топика test...");
        //Создание объекта конфигурации Spark
        SparkConf _sparkConf = new SparkConf().setAppName("app").setMaster("local[2]");
        //Создание объекта JavaStreamingContext с конфигурацией Spark и периодичностью в 2 секунды
        jssc = new JavaStreamingContext(_sparkConf, Durations.seconds(2));
        //Коллекция с параметрами брокера Kafka
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        //Отключение логов Spark
        Logger.getRootLogger().setLevel(Level.ERROR);
        //Коллекция со списком топиков Kafka
        Set<String> topicsSet = new HashSet<>();
        topicsSet.add("test");
        JavaPairInputDStream<String, String> messagesStream = null;
        try {
            //Поток вывода сообщений из брокера Kafka
            messagesStream = KafkaUtils.createDirectStream(
                    jssc,
                    String.class,
                    String.class,
                    kafka.serializer.StringDecoder.class,
                    kafka.serializer.StringDecoder.class,
                    kafkaParams,
                    topicsSet);
        } catch(Exception e) {
            System.out.println("Не удалось создать поток вывода для брокера Kafka. Скорее всего не запущен сервер Kafka или отсутствует топик test ");
            System.exit(-1);
        }

        //обработка получаемых из потока данных
        messagesStream.foreachRDD((JavaPairRDD<String, String> rdd) -> { //лямбда-выражение для объекта обработки данных
            for(Tuple2<String, String> executedMessage:rdd.collect()) {
                //Генерация случайного uuid
                String uuid = UUID.randomUUID().toString();
                //Извлечение текста из полученной пары ключ-текст
                String data = executedMessage._2();
                //В базе данных максимальный размер столбца data 1024 символа
                if (data.length() <= 1024) {
                    try {
                        //Генерация SQL-команды для добавления записи в таблицу
                        statement.executeUpdate(
                                "INSERT INTO test (id, data) VALUES ('" +
                                        uuid + "', '" +
                                        data + "');");
                    } catch (SQLException e) {
                        System.out.println("Не удалось записать данные в таблицу test. Возможно такой таблицы не существует или её столбцы не (id, data)?");
                        System.exit(-1);
                    }
                    System.out.println("Сообщение \"" + data + "\" отправлено");
                } else {
                    System.out.println("Слишком большое сообщение");
                }
            }
        });
        jssc.start();
        System.out.println("Spark Consumer успешно создан \n" +
                "Можно вводить сообщения");
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            jssc.ssc().sc().cancelAllJobs();
            jssc.stop(true, false);
        }


    }

    public void stop() {

        jssc.ssc().sc().cancelAllJobs();
        jssc.stop(true, false);
        try {
            if (connection != null) connection.close();
            if (statement != null) statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
