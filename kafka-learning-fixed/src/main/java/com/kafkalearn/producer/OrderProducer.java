package com.kafkalearn.producer;

import com.kafkalearn.config.KafkaConfig;
import com.kafkalearn.model.OrderProto.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

/**
 * =========================================================
 * УРОК 2: Kafka Producer — отправка сообщений
 *
 * Producer API предоставляет три способа отправки:
 *
 *   1. Fire-and-forget (send без callback)
 *      → Отправили и забыли, не ждём подтверждения
 *      → Самый быстрый, но можно потерять сообщения
 *
 *   2. Synchronous (send().get())
 *      → Ждём подтверждения от брокера
 *      → Медленно, но сразу знаем об ошибке
 *
 *   3. Asynchronous with callback (send(record, callback))
 *      → Callback вызывается когда брокер подтвердил
 *      → Баланс скорости и надёжности ← рекомендуется
 *
 * Структура ProducerRecord:
 *   ProducerRecord<KeyType, ValueType>(topic, key, value)
 *   - topic: куда отправляем
 *   - key: определяет партицию (null = round-robin по партициям)
 *   - value: само сообщение
 *
 * Protobuf сериализация:
 *   order.toByteArray() → byte[] → Kafka → byte[] → Order.parseFrom()
 * =========================================================
 */
public class OrderProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final KafkaProducer<String, byte[]> producer;

    public OrderProducer() {
        // Создаём producer с нашими настройками
        this.producer = new KafkaProducer<>(KafkaConfig.producerProperties());
        log.info("✅ Producer создан, подключение к: {}", KafkaConfig.BOOTSTRAP_SERVERS);
    }

    /**
     * СПОСОБ 1: Асинхронная отправка с callback (рекомендуемый).
     *
     * Kafka producer внутри батчирует сообщения и отправляет их
     * в фоновом I/O потоке. Callback вызывается в этом же потоке.
     *
     * @param order - Protobuf объект заказа
     */
    public void sendAsync(Order order) {
        // Сериализуем Protobuf объект в байты
        byte[] orderBytes = order.toByteArray();

        // Ключ = orderId → все события одного заказа попадут в одну партицию
        // Это гарантирует порядок событий для конкретного заказа
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                KafkaConfig.TOPIC_ORDERS,
                order.getOrderId(),  // ключ
                orderBytes           // значение
        );

        // Отправляем асинхронно, callback получит результат
        producer.send(record, (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                // Ошибка — брокер недоступен, сеть упала и т.д.
                log.error("❌ Ошибка отправки orderId={}: {}", order.getOrderId(), exception.getMessage());
            } else {
                // Успех — metadata содержит информацию о записи
                log.info("✅ Отправлен orderId={} → topic={}, partition={}, offset={}",
                        order.getOrderId(),
                        metadata.topic(),
                        metadata.partition(),   // в какую партицию попало
                        metadata.offset());     // какой offset получило
            }
        });
    }

    /**
     * СПОСОБ 2: Синхронная отправка — ждём подтверждения.
     *
     * Используй когда нужно знать точный offset или обработать ошибку сразу.
     * ВНИМАНИЕ: блокирует поток! Не используй в высоконагруженных сценариях.
     *
     * @return RecordMetadata с информацией о записанном сообщении
     */
    public RecordMetadata sendSync(Order order) throws Exception {
        byte[] orderBytes = order.toByteArray();

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                KafkaConfig.TOPIC_ORDERS,
                order.getOrderId(),
                orderBytes
        );

        // .get() блокирует до получения ответа от брокера
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get(); // ← здесь ждём

        log.info("✅ [SYNC] orderId={} записан: partition={}, offset={}",
                order.getOrderId(), metadata.partition(), metadata.offset());

        return metadata;
    }

    /**
     * СПОСОБ 3: Fire-and-forget — просто отправляем, не смотрим на результат.
     *
     * Используй только если потеря сообщений допустима.
     */
    public void sendFireAndForget(Order order) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                KafkaConfig.TOPIC_ORDERS,
                order.getOrderId(),
                order.toByteArray()
        );
        producer.send(record); // без callback
        log.info("🚀 [FIRE-AND-FORGET] orderId={} отправлен", order.getOrderId());
    }

    /**
     * flush() — принудительно отправить все буферизованные сообщения.
     * Kafka producer батчирует сообщения для эффективности.
     * flush() гарантирует что всё отправлено прямо сейчас.
     */
    public void flush() {
        producer.flush();
        log.info("🔄 Flush выполнен — все сообщения отправлены");
    }

    @Override
    public void close() {
        // ВАЖНО: всегда закрывай producer!
        // close() вызывает flush() внутри — гарантирует отправку всего буфера
        producer.close();
        log.info("👋 Producer закрыт");
    }
}
