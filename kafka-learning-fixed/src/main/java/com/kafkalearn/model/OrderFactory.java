package com.kafkalearn.model;

import com.kafkalearn.model.OrderProto.Order;
import com.kafkalearn.model.OrderProto.OrderStatus;

import java.util.UUID;

/**
 * Фабрика для создания тестовых Order объектов.
 *
 * Protobuf объекты создаются через Builder pattern:
 *   Order.newBuilder()
 *       .setField(value)
 *       .build()
 *
 * Изменить существующий объект нельзя — они immutable.
 * Для изменения: order.toBuilder().setStatus(newStatus).build()
 */
public class OrderFactory {

    public static Order createOrder(String customerId, String productName, double amount) {
        return Order.newBuilder()
                .setOrderId(UUID.randomUUID().toString())
                .setCustomerId(customerId)
                .setProductName(productName)
                .setAmount(amount)
                .setStatus(OrderStatus.ORDER_STATUS_CREATED)
                .setTimestampMs(System.currentTimeMillis())
                .build();
    }

    public static Order withStatus(Order original, OrderStatus newStatus) {
        // toBuilder() создаёт копию билдера с существующими полями
        return original.toBuilder()
                .setStatus(newStatus)
                .build();
    }

    // Набор тестовых заказов для демонстрации
    public static Order[] sampleOrders() {
        return new Order[]{
                createOrder("customer-001", "Ноутбук Dell XPS", 1299.99),
                createOrder("customer-002", "iPhone 15 Pro",   999.00),
                createOrder("customer-003", "Наушники Sony",   349.50),
                createOrder("customer-004", "Мышь Logitech",   79.99),
                createOrder("customer-005", "Монитор LG 4K",   699.00),
        };
    }
}
