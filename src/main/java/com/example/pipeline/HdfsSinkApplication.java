package com.example.pipeline;

import com.example.pipeline.consumer.ConsumerWorker;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class HdfsSinkApplication {

    // 연동할 카프카 클러스터 정보, 토픽 이름, 컨슈머 그룹 설정
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String TOPIC_NAME = "select-color";
    private final static String GROUP_ID = "color-hdfs-save-consumer-group";
    private final static int CONSUMER_COUNT = 3;
    private final static List<ConsumerWorker> workers = new ArrayList<>();

    public static void main(String[] args) {

        /* 안전한 컨슈머의 종료를 위해 셧다운 훅 선언
         * 셧다운 훅이 발생했을 경우 각 컨슈머 스레드에 종료를 알리도록 명시적으로 stopAndWakeup() 메서드를 호출한다. */
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 컨슈머 스레드를 스레드 풀로 관리
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < CONSUMER_COUNT; i++) {
            workers.add(new ConsumerWorker(configs, TOPIC_NAME, i));
        }

        // 컨슈머 스레드 인스턴스들을 스레드 풀에 포함시켜 실행
        workers.forEach(executorService::execute);
    }

    static class ShutdownThread extends Thread {
        @Override
        public void run() {
            log.info("Shutdown hook");
            workers.forEach(ConsumerWorker::stopAndWakeup);
        }
    }
}
