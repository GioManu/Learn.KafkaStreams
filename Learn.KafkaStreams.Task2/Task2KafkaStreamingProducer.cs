using Confluent.Kafka;
using Learn.KafkaStreams.Shared;
using Newtonsoft.Json;

namespace Learn.KafkaStreams.Task2
{
    internal class Task2KafkaStreamingProducer(ProducerConfig config, string TopicName)
        : KafkaStreamingProducer<Null, string>(config, TopicName)
    {
        public override Message<Null, string> GenerateMessage(int itValue, string topic)
        {
            return new Message<Null, string>
            {
                Value = $"Something very long #{itValue}"
            };
        }
    }
}
