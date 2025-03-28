# Event Router Service\*

The **Event Router Service** is responsible for **consuming tenant-specific events from the global Kafka broker** and **pushing them to the appropriate message broker (Kafka, RabbitMQ, MQTT, etc.) configured by the tenant**. This ensures **real-time, low-latency message routing** while maintaining **tenant isolation and security**.

---

## **📜 Service Structure Flow**

1. Service pre-loads all Message broker configurations for instant access
2. **Tenant services emit events** to the **global Kafka broker**.
3. Each message contains
    - The message payload
    - The intended binding/topic
    - The intended Message broker to be routed to
4. **Kafka partitions messages by tenant ID**.
5. **Each tenant's Event Router Service consumes only their partition**.
6. The router reads the event, and relays the message to be published by its intended message broker
