package com.kafkalearn;

import com.kafkalearn.model.OrderFactory;
import com.kafkalearn.model.OrderProto.Order;
import com.kafkalearn.model.OrderProto.OrderStatus;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit тесты для Protobuf сериализации.
 *
 * Эти тесты НЕ требуют запущенной Kafka — тестируем только
 * правильность Protobuf сериализации/десериализации.
 */
@DisplayName("Protobuf сериализация")
class ProtoSerializationTest {

    @Test
    @DisplayName("Order корректно сериализуется и десериализуется")
    void orderSerializationRoundTrip() throws InvalidProtocolBufferException {
        // Arrange
        Order original = OrderFactory.createOrder("test-customer", "Test Product", 42.50);

        // Act — сериализуем в байты (как producer)
        byte[] bytes = original.toByteArray();

        // Assert — десериализуем обратно (как consumer)
        Order deserialized = Order.parseFrom(bytes);

        assertEquals(original.getOrderId(), deserialized.getOrderId());
        assertEquals(original.getCustomerId(), deserialized.getCustomerId());
        assertEquals(original.getProductName(), deserialized.getProductName());
        assertEquals(original.getAmount(), deserialized.getAmount(), 0.001);
        assertEquals(OrderStatus.ORDER_STATUS_CREATED, deserialized.getStatus());
    }

    @Test
    @DisplayName("Protobuf компактнее JSON")
    void protobufIsSmallerThanJson() {
        Order order = OrderFactory.createOrder("customer-123", "Ноутбук Dell XPS 15", 1299.99);

        byte[] protoBytes = order.toByteArray();

        // Примерный JSON эквивалент
        String json = String.format(
                "{\"orderId\":\"%s\",\"customerId\":\"%s\",\"productName\":\"%s\",\"amount\":%.2f,\"status\":\"ORDER_STATUS_CREATED\",\"timestampMs\":%d}",
                order.getOrderId(), order.getCustomerId(), order.getProductName(),
                order.getAmount(), order.getTimestampMs()
        );

        System.out.printf("Proto size: %d bytes%n", protoBytes.length);
        System.out.printf("JSON size:  %d bytes%n", json.length());

        // Protobuf должен быть меньше JSON
        assertTrue(protoBytes.length < json.length(),
                "Protobuf (" + protoBytes.length + " bytes) должен быть меньше JSON (" + json.length() + " bytes)");
    }

    @Test
    @DisplayName("withStatus создаёт новый объект с обновлённым статусом")
    void withStatusCreatesUpdatedCopy() {
        Order created = OrderFactory.createOrder("customer-1", "Product", 100.0);
        assertEquals(OrderStatus.ORDER_STATUS_CREATED, created.getStatus());

        Order paid = OrderFactory.withStatus(created, OrderStatus.ORDER_STATUS_PAID);

        // Оригинал не изменился (immutable!)
        assertEquals(OrderStatus.ORDER_STATUS_CREATED, created.getStatus());
        // Новый объект с обновлённым статусом
        assertEquals(OrderStatus.ORDER_STATUS_PAID, paid.getStatus());
        // orderId сохранился
        assertEquals(created.getOrderId(), paid.getOrderId());
    }

    @Test
    @DisplayName("Invalid bytes выбрасывают InvalidProtocolBufferException")
    void invalidBytesThrowException() {
        byte[] garbage = {0x01, 0x02, (byte) 0xFF, 0x00};

        // Protobuf не может десериализовать мусор — именно это ловит наш consumer
        assertThrows(InvalidProtocolBufferException.class, () -> Order.parseFrom(garbage));
    }
}
