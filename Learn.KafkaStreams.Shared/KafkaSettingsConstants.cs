using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Learn.KafkaStreams.Shared
{
    public class KafkaSettingsConstants
    {
        public const string Task1Topic1 = "task1-1";
        public const string Task1Topic2 = "task1-2";
        public const string Task2Topic1 = "task2";
        public const string Task3Topic1 = "task3-1";
        public const string Task3Topic2 = "task3-2";
        public const string Task4Topic1 = "task4";

        public const string BootStrapServers = "127.0.0.1:8097,127.0.0.1:8098,127.0.0.1:8099";

        public static ProducerConfig ProducerConfig() => new()
        {
            BootstrapServers = BootStrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageSendMaxRetries = int.MaxValue,
            MessageTimeoutMs = 10_000
        };

        public static string AppId(string taskId) => $"{taskId}-{Guid.NewGuid()}-app-id";
    }
}
