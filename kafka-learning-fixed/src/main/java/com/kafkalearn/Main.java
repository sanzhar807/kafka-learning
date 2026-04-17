package com.kafkalearn;

import com.kafkalearn.consumer.OrderConsumer;
import com.kafkalearn.model.OrderFactory;
import com.kafkalearn.model.OrderProto.Order;
import com.kafkalearn.model.OrderProto.OrderStatus;
import com.kafkalearn.producer.OrderProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * =========================================================
 * ГЛАВНЫЙ КЛАСС — точка входа для всех демонстраций
 *
 * ПЕРЕД ЗАПУСКОМ:
 *   1. Запусти Kafka через Docker:
 *      docker-compose up -d
 *
 *   2. Дождись пока Kafka поднимется (~15-20 секунд):
 *      docker-compose logs -f kafka
 *
 * ЗАПУСК:
 *   ./gradlew run --args="produce"   → отправить тестовые заказы
 *   ./gradlew run --args="consume"   → читать заказы (Ctrl+C для остановки)
 *   ./gradlew run --args="demo"      → продюсим, потом читаем 10 секунд
 *
 * ПОЛЕЗНЫЕ КОМАНДЫ:
 *   # Список топиков
 *   docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
 *
 *   # Детали топика
 *   docker exec kafka kafka-topics.sh --describe --topic orders-topic --bootstrap-server localhost:9092
 *
 *   # Consumer group lag
 *   docker exec kafka kafka-consumer-groups.sh \
 *     --describe --group orders-consumer-group --bootstrap-server localhost:9092
 * =========================================================
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "demo";

        switch (mode) {
            case "produce" -> runProducer();
            case "consume" -> runConsumer();
            case "demo"    -> runFullDemo();
            default        -> printHelp();
        }
    }

    /**
     * СЦЕНАРИЙ 1: Только продюсинг.
     * Запуск: ./gradlew run --args="produce"
     */
    static void runProducer() throws Exception {
        log.info("=== 🚀 ЗАПУСК PRODUCER DEMO ===");

        try (OrderProducer producer = new OrderProducer()) {
            Order[] orders = OrderFactory.sampleOrders();

            log.info("--- Способ 1: Асинхронная отправка с callback ---");
            for (Order order : orders) {
                producer.sendAsync(order);
            }

            log.info("--- Способ 2: Синхронная отправка ---");
            Order syncOrder = OrderFactory.createOrder("customer-sync", "Планшет Samsung", 599.00);
            producer.sendSync(syncOrder);

            log.info("--- Способ 3: Fire-and-forget ---");
            Order fogOrder = OrderFactory.createOrder("customer-fog", "Клавиатура Keychron", 149.00);
            producer.sendFireAndForget(fogOrder);

            log.info("--- Обновление статуса заказа ---");
            Order paid = OrderFactory.withStatus(orders[0], OrderStatus.ORDER_STATUS_PAID);
            producer.sendAsync(paid);

            producer.flush();
            log.info("✅ Все сообщения отправлены!");
        }
    }

    /**
     * СЦЕНАРИЙ 2: Только консюминг (бесконечный, Ctrl+C для остановки).
     * Запуск: ./gradlew run --args="consume"
     */
    static void runConsumer() {
        log.info("=== 📥 ЗАПУСК CONSUMER DEMO ===");
        log.info("Нажми Ctrl+C для остановки");

        OrderConsumer consumer = new OrderConsumer();

        // Graceful shutdown по Ctrl+C — shutdown hook вызывается в отдельном потоке
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("🛑 Получен сигнал остановки...");
            consumer.stop();
        }));

        try {
            consumer.startConsuming(); // блокирующий вызов до stop()
        } finally {
            consumer.close();
        }
    }

    /**
     * СЦЕНАРИЙ 3: Полная демонстрация — продюсим, потом читаем 10 секунд.
     * Запуск: ./gradlew run  (или ./gradlew run --args="demo")
     *
     * FIX: startConsuming() — бесконечный цикл, поэтому запускаем его
     * в отдельном потоке и останавливаем через stop() после таймаута.
     */
    static void runFullDemo() throws Exception {
        log.info("=== 🎓 ПОЛНАЯ ДЕМОНСТРАЦИЯ KAFKA ===");

        // ШАГ 1: Продюсим тестовые сообщения
        log.info("\n--- ШАГ 1: Отправляем заказы в Kafka ---");
        try (OrderProducer producer = new OrderProducer()) {
            for (Order order : OrderFactory.sampleOrders()) {
                producer.sendAsync(order);
                Thread.sleep(100);
            }
            producer.flush();
        }
        log.info("✅ Продюсинг завершён!\n");

        // ШАГ 2: Читаем сообщения в отдельном потоке с таймаутом
        log.info("--- ШАГ 2: Читаем заказы из Kafka (10 секунд) ---");
        OrderConsumer consumer = new OrderConsumer();

        // FIX: запускаем consumer в отдельном потоке, иначе main заблокируется навсегда
        Thread consumerThread = new Thread(() -> {
            try {
                consumer.startConsuming();
            } finally {
                consumer.close();
            }
        }, "kafka-consumer-thread");
        consumerThread.setDaemon(true);
        consumerThread.start();

        // Даём консюмеру 10 секунд прочитать сообщения
        Thread.sleep(10_000);

        // Останавливаем gracefully
        log.info("⏹️ Останавливаем consumer после демо...");
        consumer.stop();
        consumerThread.join(5_000); // ждём завершения потока максимум 5 сек

        log.info("✅ Демонстрация завершена!");
    }

    static void printHelp() {
        System.out.println("""
                Kafka Learning Project — доступные режимы:
                
                  ./gradlew run --args="produce"   → отправить тестовые заказы
                  ./gradlew run --args="consume"   → читать заказы из Kafka
                  ./gradlew run --args="demo"      → полная демонстрация
                
                Убедись что Kafka запущена:
                  docker-compose up -d
                """);
    }
}
