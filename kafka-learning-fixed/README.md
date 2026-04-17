# 🎓 Kafka Learning Project

Учебный Java проект для изучения Apache Kafka с Protobuf сериализацией.

## 📚 Структура уроков

```
src/
├── main/
│   ├── proto/
│   │   └── order.proto              ← УРОК 1: Схема Protobuf сообщения
│   ├── java/com/kafkalearn/
│   │   ├── config/
│   │   │   └── KafkaConfig.java     ← УРОК 2: Конфигурация Producer/Consumer
│   │   ├── producer/
│   │   │   └── OrderProducer.java   ← УРОК 3: Три способа отправки сообщений
│   │   ├── consumer/
│   │   │   └── OrderConsumer.java   ← УРОК 4: Poll-цикл, offset management
│   │   ├── model/
│   │   │   └── OrderFactory.java    ← Хелпер для создания тестовых объектов
│   │   └── Main.java                ← Точка входа, демо-сценарии
└── test/
    └── ProtoSerializationTest.java  ← Unit тесты (без Kafka)
```

## 🚀 Быстрый старт

### 1. Запустить Kafka
```bash
docker-compose up -d
```

Открой **Kafka UI** в браузере: http://localhost:8080  
Там можно видеть топики, сообщения, consumer groups в реальном времени!

### 2. Собрать проект
```bash
./gradlew build
```
> Protobuf плагин автоматически генерирует Java классы из `.proto` файлов  
> Сгенерированный код появится в: `build/generated/source/proto/main/java/`

### 3. Запустить демо
```bash
# Полная демонстрация (продюсим + консюмим)
./gradlew run

# Только продюсинг
./gradlew run --args="produce"

# Только консюминг (Ctrl+C для остановки)
./gradlew run --args="consume"
```

### 4. Запустить тесты (без Kafka!)
```bash
./gradlew test
```

---

## 📖 Ключевые концепции

### Kafka архитектура
```
Producer ──→ [Topic: orders-topic]
               ├── Partition 0: msg0, msg3, msg6 ...
               ├── Partition 1: msg1, msg4, msg7 ...
               └── Partition 2: msg2, msg5, msg8 ...
                        ↓
              Consumer Group "orders-consumer-group"
               ├── Consumer A → читает Partition 0
               ├── Consumer B → читает Partition 1
               └── Consumer C → читает Partition 2
```

### Ключ сообщения и партиционирование
| Ключ | Поведение |
|------|-----------|
| `null` | Round-robin по партициям |
| `"customer-001"` | Всегда в одну партицию → гарантия порядка для этого клиента |

### Способы отправки (Producer)
| Способ | Скорость | Надёжность | Когда использовать |
|--------|----------|------------|-------------------|
| Fire-and-forget | ⚡⚡⚡ | ❌ | Логи, метрики (потеря OK) |
| Async + callback | ⚡⚡ | ✅ | Большинство случаев |
| Sync (`.get()`) | ⚡ | ✅✅ | Нужен точный offset |

### Offset commit стратегии
```
Сообщения:  [A] [B] [C] [D] [E]
             offset: 0  1  2  3  4

At-most-once:   commit → process  (сначала коммит, потом обработка)
                Если упали после коммита — сообщение потеряно

At-least-once:  process → commit  (сначала обработка, потом коммит)  ← наш код
                Если упали после обработки но до коммита — получим дважды
                Решение: идемпотентная обработка

Exactly-once:   Kafka транзакции (сложнее, для продакшена)
```

### Protobuf vs JSON
```
Order { orderId: "abc-123", customer: "customer-001", product: "Laptop", amount: 1299.99 }

JSON:    {"orderId":"abc-123","customerId":"customer-001","productName":"Laptop","amount":1299.99}  → ~85 байт
Protobuf: бинарное представление                                                                   → ~35 байт
```

---

## 🛠️ Полезные команды Docker

```bash
# Список топиков
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Создать топик вручную с 3 партициями
docker exec kafka kafka-topics.sh --create \
  --topic orders-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Детали топика
docker exec kafka kafka-topics.sh --describe \
  --topic orders-topic \
  --bootstrap-server localhost:9092

# Consumer group lag (отставание)
docker exec kafka kafka-consumer-groups.sh \
  --describe \
  --group orders-consumer-group \
  --bootstrap-server localhost:9092

# Читать сообщения напрямую (как строки, без Protobuf)
docker exec kafka kafka-console-consumer.sh \
  --topic orders-topic \
  --bootstrap-server localhost:9092 \
  --from-beginning

# Сбросить offset группы на начало (перечитать все сообщения)
docker exec kafka kafka-consumer-groups.sh \
  --group orders-consumer-group \
  --topic orders-topic \
  --reset-offsets \
  --to-earliest \
  --execute \
  --bootstrap-server localhost:9092
```

---

## 🧪 Эксперименты для изучения

### Эксперимент 1: Несколько консюмеров
Запусти два терминала с `./gradlew run --args="consume"` одновременно.  
Kafka распределит партиции между ними. В Kafka UI видно кто читает что.

### Эксперимент 2: Consumer lag
1. Продюс много сообщений: несколько раз `./gradlew run --args="produce"`
2. Смотри lag командой выше — увидишь накопленные сообщения
3. Запусти консюмер — lag начнёт уменьшаться

### Эксперимент 3: Partition ordering
Все заказы `customer-001` попадут в одну партицию (ключ = orderId).  
Измени ключ на `customerId` в `OrderProducer` и проверь поведение.

### Эксперимент 4: Изменить .proto файл
Добавь новое поле в `order.proto` (с новым номером, например `= 7`).  
Пересобери проект — существующие сообщения в Kafka читаются без ошибок!  
Это и есть backward compatibility Protobuf.
