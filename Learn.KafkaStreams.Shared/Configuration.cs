using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Net.Mime.MediaTypeNames;

namespace Learn.KafkaStreams.Shared
{
    public record Configuration(string ApplicationId, string BootstrapServers);
    public record Task1Configuration(string ApplicationId, string BootstrapServers, string ConsumerTopic, string ProducerTopic) : Configuration(ApplicationId, BootstrapServers);
    public record Task2Configuration(string ApplicationId, string BootstrapServers, string Topic) : Configuration(ApplicationId, BootstrapServers);
    public record Task3Configuration(string ApplicationId, string BootstrapServers, string[] Topics) : Configuration(ApplicationId, BootstrapServers);
    public record Task4Configuration(string ApplicationId, string BootstrapServers, string Topic) : Configuration(ApplicationId, BootstrapServers);
}
