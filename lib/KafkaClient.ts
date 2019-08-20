import { KafkaConfig } from "./KafkaConfig";
import { KafkaConsumer, ConsumerStreamMessage, Producer } from "node-rdkafka";

/**
 * Represents kafka client wrapper.
 */
export class KafkaClient {
    constructor(
        private config: KafkaConfig,
        private consumer: KafkaConsumer = null,
        private producer: Producer = null,
        private listeners: Map<string, KafkaListener> = new Map(),
    ) { }

    /**
     * Creates the kafka consumer.
     * 
     * @param topics list of kafka topics
     */
    createConsumer(topics: string[]): Promise<void> {
        if (this.consumer) {
            return;
        }

        const globalConfig = {
            "metadata.broker.list": this.config.brokerList,
            "group.id": this.config.groupId,
            "offset_commit_cb": (error: any, topicPartitions: any) => {
                if (error) {
                    console.error(`Kafka committing error: ${error}`);
                }
            },
            "socket.keepalive.enable": true,
            "enable.auto.commit": this.config.autoCommitEnabled || true
        };

        const topicConfig = {};

        this.consumer = new KafkaConsumer(globalConfig, topicConfig);
        this.consumer.connect();
        this.consumer.on("disconnected", () => {
            console.log("Kafka consumer has been disconnected");
        });
        this.consumer.on("data", (message: ConsumerStreamMessage) => {
            if (this.listeners.has(message.topic)) {
                this.listeners.get(message.topic)(JSON.parse(message.value.toString()));
            }
        });
        return new Promise(resolve => {
            this.consumer.on("ready", () => {
                console.log("Kafka consumer ready");
                this.consumer.subscribe(topics);
                this.consumer.consume();
                resolve();
            });
        });
    }

    /**
     * Creates the kafka producer.
     */
    createProducer(): Promise<void> {
        if (this.producer) {
            return;
        }

        const globalConfig = {
            "metadata.broker.list": this.config.brokerList,
            "client.id": this.config.clientId,
            "socket.keepalive.enable": true,
            "compression.codec": "gzip",
            "retry.backoff.ms": this.config.retryBackoffMs || 200,
            "message.send.max.retries": this.config.maxRetries || 10,
            "queue.buffering.max.messages": 100000,
            "queue.buffering.max.ms": 1000,
            "batch.num.messages": 1000000,
            "dr_cb": true
        };

        const topicConfig = {};

        this.producer = new Producer(globalConfig, topicConfig);
        this.producer.connect();
        this.producer.on("event.error", (err) => {
            console.error(err);
        });
        this.producer.on("delivery-report", (err: Error, report: any) => {
            if (err) {
                console.error(err);
            } else {
                console.log("delivery-report: " + JSON.stringify(report));
            }
        });
        return new Promise(resolve => {
            this.producer.on("ready", () => {
                console.log("Kafka producer ready");
                resolve();
            });
        });
    }

    /**
     * Sends a message to kafka.
     * 
     * @param topic topic name
     * @param message message object
     */
    sendMessage(topic: string, message: any): void {
        if (!this.producer) {
            throw new Error("Kafka producer is not initialized");
        }
        try {
            const value = Buffer.from(JSON.stringify(message));
            this.producer.produce(topic, -1, value, null, Date.now());
        } catch (err) {
            console.error(err);
        }
    }

    /**
     * Subscribes on kafka topic.
     * 
     * @param topicName topic name
     * @param callback kafka listener
     */
    subscribe(topicName: string, callback: KafkaListener): void {
        this.listeners.set(topicName, callback);
    }

    /**
     * Disconnects of kafka consumer and producer. 
     */
    disconnect(): void {
        if (this.consumer) {
            this.consumer.disconnect();
        }
        if (this.producer) {
            this.producer.disconnect();
        }
    }
}

export type KafkaListener = (message: any) => void;
