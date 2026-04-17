package com.kafkalearn.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * =========================================================
 * УРОК 1: Конфигурация Kafka Producer и Consumer
 *
 * Kafka работает по принципу:
 *   Producer --> [Topic/Partition] --> Consumer Group
 *
 * Основные понятия:
 *   - Bootstrap servers: адреса брокеров для первого подключения
 *   - Topic: именованный лог сообщений (как таблица в БД)
 *   - Partition: раздел топика (параллельность = кол-во партиций)
 *   - Consumer Group: группа консюмеров, читающих один топик
 *     каждый консюмер в группе читает свои партиции
 *   - Offset: позиция сообщения в партиции (начиная с 0)
 * =========================================================
 */
public class KafkaConfig {

    // Адрес брокера — для локального запуска через Docker
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

    // Название топика
    public static final String TOPIC_ORDERS = "orders-topic";

    // Группа консюмеров — важно! Разные группы читают независимо
    public static final String CONSUMER_GROUP = "orders-consumer-group";

    /**
     * Конфигурация PRODUCER.
     *
     * Producer отправляет сообщения в Kafka.
     * Каждое сообщение = ключ + значение (оба могут быть null).
     * Ключ определяет партицию (одинаковый ключ → одна партиция → порядок гарантирован).
     */
    public static Properties producerProperties() {
        Properties props = new Properties();

        // Куда подключаться (достаточно одного, Kafka найдет остальные)
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // Сериализаторы — как превращать объект в байты для Kafka
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // acks — подтверждение записи:
        //   "0" = fire-and-forget (быстро, но можно потерять)
        //   "1" = только leader (баланс)
        //   "all" = все реплики (медленно, но надёжно) ← рекомендуется для продакшена
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Retry при временных ошибках (брокер недоступен и т.д.)
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Идемпотентность — гарантия "exactly once" для producer
        // Защищает от дублей при retry
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Батчинг — producer накапливает сообщения и отправляет пачкой
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);      // 16 KB батч
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);           // ждать до 5ms для батча

        // Буфер памяти для ещё не отправленных сообщений
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 32 MB

        return props;
    }

    /**
     * Конфигурация CONSUMER.
     *
     * Consumer читает сообщения из партиций.
     * Важно: Consumer Group обеспечивает масштабирование:
     *   - 1 топик с 3 партициями + 3 консюмера = каждый читает свою партицию
     *   - Если консюмеров больше чем партиций — лишние будут idle
     */
    public static Properties consumerProperties() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // ID группы — ключевой параметр!
        // Kafka хранит offsets per group, поэтому разные группы читают независимо
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);

        // Десериализаторы — как превращать байты обратно в объекты
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        // auto.offset.reset — что делать если offset не найден (новая группа):
        //   "earliest" = читать с самого начала топика
        //   "latest"   = читать только новые сообщения (default)
        //   "none"     = упасть с ошибкой
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Автоматический коммит offset (true = Kafka сама сохраняет прогресс)
        // false = ручной коммит (больше контроля, рекомендуется для продакшена)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Максимум записей за один poll() вызов
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

        // Таймаут сессии — если консюмер не шлёт heartbeat дольше, он считается мёртвым
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);

        return props;
    }
}
