export interface KafkaConfig {
    brokerList: string;
    groupId: string;
    clientId: string;
    autoCommitEnabled?: boolean;
    maxRetries?: number;
    retryBackoffMs?: number;
}
