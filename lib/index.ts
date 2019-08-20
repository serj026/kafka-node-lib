import { KafkaClient } from "./KafkaClient";
import { KafkaConfig } from "./KafkaConfig";

let kafkaClient: KafkaClient;

/**
 * Creates the kafka client.
 * 
 * @param config kafka config
 */
export const create = (config: KafkaConfig): KafkaClient => {
    kafkaClient = new KafkaClient(config);
    return kafkaClient;
};

/**
 * Returns the kafka client.
 */
export const getClient = (): KafkaClient => {
    return kafkaClient;
};
