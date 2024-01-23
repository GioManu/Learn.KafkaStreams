using Confluent.Kafka;
using Learn.KafkaStreams.Shared;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Confluent.Kafka.ConfigPropertyNames;

namespace Learn.KafkaStreams.Task3
{
    internal class Task3KafkaStreamingProducer(ProducerConfig config, params string[] Topics)
        : KafkaStreamingProducer<Null, string>(config, Topics)
    {
        public override Message<Null, string> GenerateMessage(int itValue, string topic)
        {
            return new Message<Null, string>
            {
                Value = $"{itValue}:{topic}'s value"
            };
        }
    }
}
