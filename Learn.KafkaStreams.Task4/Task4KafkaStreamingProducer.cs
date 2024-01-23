using Confluent.Kafka;
using Learn.KafkaStreams.Shared;
using Newtonsoft.Json;

namespace Learn.KafkaStreams.Task4
{
    internal class Task4KafkaStreamingProducer(ProducerConfig config, string TopicName)
        : KafkaStreamingProducer<Null, string>(config, TopicName)
    {
        public override Message<Null, string> GenerateMessage(int itValue, string topic)
        {
            var employee = new Employee($"Company-{Guid.NewGuid()}", $"Employee-{Guid.NewGuid()}-Name", $"Position#{itValue}", (short)itValue);

            return new Message<Null, string>
            {
                Value = JsonConvert.SerializeObject(employee)
            };
        }
    }
}
