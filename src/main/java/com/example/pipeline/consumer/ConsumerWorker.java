package com.example.pipeline.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

@Slf4j
public class ConsumerWorker implements Runnable {

    /* 컨슈머의 poll() 메서드를 통해 전달받은 데이터를 임시 저장하는 버퍼
     * static 변수로 선언하여 다수의 스레드가 만들어지더라도 동일 변수에 접근한다.
     * 다수 스레드가 동시 접근할 수 있는 변수이므로 멀티 스레드 환경에서 안전하게 사용할 수 있는 ConcurrentHashMap으로 구현
     * 파티션 번호와 메시지 값들이 들어간다.*/
    private static Map<Integer, List<String>> bufferString = new ConcurrentHashMap<>();
    // 오프셋 값을 저장하고 파일 이름을 저장할 대 오프셋 번호를 붙이는 데에 사용
    private static Map<Integer, Long> currentFileOffset = new ConcurrentHashMap<>();

    private final static int FLUSH_RECORD_COUNT = 10;
    private Properties prop;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;

    public ConsumerWorker(Properties prop, String topic, int number) {
        log.info("Generate ConsumerWorker.");
        this.prop = prop;
        this.topic = topic;
        // 스레드에 이름을 붙임으로써 로깅 시에 편하게 스레드 번호를 확인할 수 있다.
        this.threadName = "consumer-thread-" + number;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(threadName);

        // 스레드를 생성하는 HdfsSinkApplication에서 설정한 컨슈머 설정을 가져와서 KafkaConsumer 인스턴스를 생성
        consumer = new KafkaConsumer<>(prop);
        // 토픽 구독
        consumer.subscribe(Arrays.asList(topic));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    // poll() 메서드로 가져온 데이터들을 버퍼에 쌓기 위한 addHdfsFileBuffer() 호출
                    addHdfsFileBuffer(record);
                }

                /* 한 번 polling이 완료되고 나면 saveBufferToHdfsFile() 호출
                 * 버퍼에 쌓인 데이터가 일정 개수 이상 쌓였을 경우 HDFS에 저장하는 로직 수행
                 * consumer.assignment()를 파라미터로 넘김으로써 현재 컨슈머 스레드에 할당된 파티션에 대한 버퍼 데이터만 적재할 수 있도록 한다. */
                saveBufferToHdfsFile(consumer.assignment());
            }
        } catch (WakeupException e) {
            // 안전한 종료를 위해 WakeupException을 받아서 컨슈머 종료 과정 수행
            log.warn("Wakeup consumer");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            consumer.close();
        }
    }

    /**
     * 레코드를 받아서 메시지 값을 버퍼에 넣는다.
     */
    private void addHdfsFileBuffer(ConsumerRecord<String, String> record) {
        List<String> buffer = bufferString.getOrDefault(record.partition(), new ArrayList<>());
        buffer.add(record.value());
        bufferString.put(record.partition(), buffer);

        /* 버퍼 크기가 1이라면 버퍼의 가장 처음 오프셋이라는 뜻이므로 currentFileOffset 변수에 세팅
         * currentFileOffset 변수에서 오프셋 번호를 관리하면 추후 파일을 저장할 때 파티션 이름과 오프셋 번호를 붙여서 저장할 수 있기 때문에 이슈 발생 시 파티션과 오프셋에 대한 정보를 알 수 있다. */
        if (buffer.size() == 1) {
            currentFileOffset.put(record.partition(), record.offset());
        }
    }

    /**
     * 버퍼의 데이터가 flush될 만큼 개수가 충족되었는지 확인
     * 컨슈머로부터 Set<TopicPartition> 정보를 받아서 컨슈머 스레드에 할당된 파티션에만 접근
     */
    private void saveBufferToHdfsFile(Set<TopicPartition> partitions) {
        partitions.forEach(p -> checkFlushCount(p.partition()));
    }

    /**
     * 파티션 번호의 버퍼를 확인하여 flush를 수행할 만큼 레코드 개수가 찼는지 확인
     * 만약 일정 개수 이상이면 HDFS 적재 로직인 save() 메서드 호출
     */
    private void checkFlushCount(int partitionNo) {
        if (bufferString.get(partitionNo) != null) {
            if (bufferString.get(partitionNo).size() >= FLUSH_RECORD_COUNT) {
                save(partitionNo);
            }
        }
    }

    /**
     * 실질적인 HDFS 적재를 수행하는 메서드
     */
    private void save(int partitionNo) {
        if (!bufferString.get(partitionNo).isEmpty()) {
            try {
                String fileName = "/data/color-" + partitionNo + "-" + currentFileOffset.get(partitionNo) + ".log";

                /* HDFS 적재를 위한 설정 수행
                * 로컬 환경에 HDFS를 설치했다는 가정하에 hdfs://localhost:9000을 대상으로 FileSystem 인스턴스 생성 */
                Configuration configuration = new Configuration();
                configuration.set("fs.defaultFS", "hdfs://localhost:9000");
                FileSystem hdfsFileSystem = FileSystem.get(configuration);

                /* 버퍼 데이터를 파일로 저장하기 위해 FSDataOutputStream 인스턴스 생성
                * bufferString에 쌓인 버퍼 데이터를 fileOutputStream에 저장
                * 저장이 완료된 이후에는 close() 메서드를 통해 안전하게 종료 */
                FSDataOutputStream fileOutputStream = hdfsFileSystem.create(new Path(fileName));
                fileOutputStream.writeBytes(StringUtils.join("\n", bufferString.get(partitionNo)));
                fileOutputStream.close();

                // 버퍼 데이터 적재 완료되었다면 새로운 ArrayList를 선언하여 버퍼 데이터를 초기화한다.
                bufferString.put(partitionNo, new ArrayList<>());
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 셧다운 훅이 발생했을 때 안전한 종료를 위해 consumer에 wakeup() 메서드 호출
     * 남은 버퍼의 데이터를 모두 저장하기 위해 saveRemainBufferToHdfsFile() 메서드도 호출
     */
    public void stopAndWakeup() {
        log.info("stopAndWakeup");
        consumer.wakeup();
        saveRemainBufferToHdfsFile();
    }

    /**
     * 버퍼에 남아있는 모든 데이터를 저장하기 위한 메서드
     * 컨슈머 스레드 종료 시에 호출
     */
    private void saveRemainBufferToHdfsFile() {
        bufferString.forEach((partitionNo, v) -> this.save(partitionNo));
    }
}
