package com.kafkalearn.consumer;

import com.kafkalearn.config.KafkaConfig;
import com.kafkalearn.model.OrderProto.Order;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * =========================================================
 * УРОК 3: Kafka Consumer — чтение сообщений
 *
 * Consumer читает сообщения через poll() цикл.
 *
 * Жизненный цикл:
 *   1. subscribe(topics) — подписываемся на топики
 *   2. poll(timeout) — запрашиваем новые сообщения
 *   3. обрабатываем записи
 *   4. commitSync/commitAsync — сохраняем прогресс (offset)
 *   5. повторяем с шага 2
 *
 * Offset management:
 *   - Kafka хранит "committed offset" для каждой (group, partition) пары
 *   - После restart консюмер продолжает с последнего committed offset
 *   - auto.commit = true → Kafka сама коммитит каждые 5 секунд (рискованно!)
 *   - auto.commit = false → ты контролируешь когда коммитить (рекомендуется)
 *
 * At-least-once vs At-most-once vs Exactly-once:
 *   - Коммит ДО обработки → at-most-once (сообщение может потеряться)
 *   - Коммит ПОСЛЕ обработки → at-least-once (может прийти дважды при падении)
 *   - Exactly-once → нужна идемпотентная обработка или транзакции
 * =========================================================
 */
public class OrderConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private final KafkaConsumer<String, byte[]> consumer;
    private final AtomicBoolean running = new AtomicBoolean(false);
    // FIX: флаг — был ли subscribe вызван, чтобы не делать commitSync без подписки
    private volatile boolean subscribed = false;

    public OrderConsumer() {
        this.consumer = new KafkaConsumer<>(KafkaConfig.consumerProperties());
    }

    /**
     * Подписываемся на топик и запускаем poll-цикл.
     *
     * subscribe() vs assign():
     *   - subscribe(topics) → Kafka сама распределяет партиции между консюмерами в группе
     *     Это нужно для автоматического масштабирования (Consumer Group Rebalance)
     *   - assign(partitions) → ручное назначение конкретных партиций
     *     Используется когда нужен полный контроль
     */
    public void startConsuming() {
        consumer.subscribe(Collections.singletonList(KafkaConfig.TOPIC_ORDERS));
        subscribed = true;
        log.info("📋 Подписались на топик: {}", KafkaConfig.TOPIC_ORDERS);

        running.set(true);

        // FIX: WakeupException — штатный способ прервать poll() извне (через stop()).
        // Её НУЖНО поймать снаружи цикла, а не внутри — иначе цикл не прервётся.
        try {
            while (running.get()) {
                // poll() — запрашиваем новые сообщения.
                // Duration.ofMillis(1000) — максимальное время ожидания если сообщений нет.
                // Важно: poll() также поддерживает heartbeat с координатором группы!
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    log.debug("⏳ Новых сообщений нет, ждём...");
                    continue;
                }

                log.info("📦 Получено {} сообщений", records.count());

                for (ConsumerRecord<String, byte[]> record : records) {
                    processRecord(record);
                }

                // РУЧНОЙ КОММИТ после успешной обработки ВСЕГО батча.
                // commitSync() — блокирует до подтверждения от брокера (надёжно).
                // commitAsync() — не блокирует, callback при завершении (быстро).
                commitOffsets(records);
            }
        } catch (WakeupException e) {
            // FIX: WakeupException — это НЕ ошибка, а штатный сигнал остановки от stop().
            // Просто выходим из цикла. Не логируем как error.
            if (running.get()) {
                // Если running=true и WakeupException — это неожиданно, пробрасываем
                throw e;
            }
            log.info("⏹️ Poll прерван сигналом остановки");
        }

        log.info("🛑 Consumer цикл завершён");
    }

    /**
     * Обработка одной записи из Kafka.
     *
     * ConsumerRecord содержит:
     *   - topic()     → название топика
     *   - partition() → номер партиции (0, 1, 2, ...)
     *   - offset()    → позиция в партиции (монотонно растущий номер)
     *   - key()       → ключ (тот что задал producer)
     *   - value()     → само сообщение (байты)
     *   - timestamp() → когда создано
     *   - headers()   → метаданные (аналог HTTP headers)
     */
    private void processRecord(ConsumerRecord<String, byte[]> record) {
        log.info("🔍 Обработка: topic={}, partition={}, offset={}, key={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key());

        try {
            // Десериализация Protobuf: byte[] → Order объект
            Order order = Order.parseFrom(record.value());

            log.info("📬 Заказ получен: id={}, customer={}, product={}, amount={}, status={}",
                    order.getOrderId(),
                    order.getCustomerId(),
                    order.getProductName(),
                    order.getAmount(),
                    order.getStatus());

            // Здесь твоя бизнес-логика:
            // - сохранить в БД
            // - отправить уведомление
            // - вызвать другой сервис
            handleOrderBusiness(order);

        } catch (InvalidProtocolBufferException e) {
            // Ошибка десериализации — "poison pill" сообщение
            // Не останавливаем консюмер! Логируем и двигаемся дальше
            log.error("❌ Невозможно десериализовать сообщение offset={}: {}",
                    record.offset(), e.getMessage());
        }
    }

    /**
     * Пример бизнес-логики обработки заказа.
     */
    private void handleOrderBusiness(Order order) {
        switch (order.getStatus()) {
            case ORDER_STATUS_CREATED -> log.info("🆕 Новый заказ {} создан!", order.getOrderId());
            case ORDER_STATUS_PAID -> log.info("💰 Заказ {} оплачен, начинаем сборку", order.getOrderId());
            case ORDER_STATUS_SHIPPED -> log.info("📦 Заказ {} отправлен", order.getOrderId());
            case ORDER_STATUS_DELIVERED -> log.info("✅ Заказ {} доставлен клиенту", order.getOrderId());
            case ORDER_STATUS_CANCELLED -> log.info("❌ Заказ {} отменён, возврат средств", order.getOrderId());
            default -> log.warn("⚠️ Неизвестный статус заказа {}", order.getOrderId());
        }
    }

    /**
     * Ручной коммит offset — сообщаем Kafka что обработали до этого места.
     *
     * commitSync vs commitAsync:
     *   commitSync()  → блокирует, повторяет при ошибке → надёжно, медленнее
     *   commitAsync() → не блокирует, callback → быстро, нет автоматических retry
     *
     * Лучшая практика для продакшена:
     *   commitAsync() в цикле + commitSync() перед shutdown
     */
    private void commitOffsets(ConsumerRecords<String, byte[]> records) {
        try {
            // Строим карту offset+1 для каждой партиции
            // (следующий offset который нужно прочитать = текущий + 1)
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            for (ConsumerRecord<String, byte[]> record : records) {
                offsets.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1) // +1 = следующий для чтения
                );
            }

            // Синхронный коммит — ждём подтверждения
            consumer.commitSync(offsets);
            log.debug("✅ Offsets закоммичены: {}", offsets);

        } catch (Exception e) {
            log.error("❌ Ошибка коммита offsets: {}", e.getMessage());
        }
    }

    /**
     * Остановить consumer gracefully.
     * Вызывает wakeup() который прерывает текущий poll() с WakeupException.
     */
    public void stop() {
        log.info("⏹️ Останавливаем consumer...");
        running.set(false);
        consumer.wakeup(); // Прерывает блокирующий poll()
    }

    @Override
    public void close() {
        try {
            // FIX: commitSync только если был subscribe — иначе IllegalStateException
            if (subscribed) {
                consumer.commitSync();
            }
        } catch (Exception e) {
            log.warn("⚠️ Финальный commitSync не удался: {}", e.getMessage());
        } finally {
            consumer.close();
            log.info("👋 Consumer закрыт");
        }
    }
}
