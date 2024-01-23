using Confluent.Kafka;
using Learn.KafkaStreams.Shared;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Learn.KafkaStreams.Task1
{
    internal class Task1KafkaStreamingProducer(ProducerConfig config, string TopicName) 
        : KafkaStreamingProducer<Null, string>(config, TopicName)
    {
        public override Message<Null, string> GenerateMessage(int itValue, string topic)
        {
            return new Message<Null, string>
            {
                Value = JsonConvert.SerializeObject(new Message($"auto generated message #{itValue}"))
            };
        }
    }
}
