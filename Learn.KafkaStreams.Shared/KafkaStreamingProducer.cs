using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Confluent.Kafka.ConfigPropertyNames;

namespace Learn.KafkaStreams.Shared
{
    public class KafkaStreamingProducer<TKey, TValue>(ProducerConfig config, params string[] Topics)
    {
        public async Task<int> ProduceMessagesAsync(int start, int end)
        {
            using var producer = new ProducerBuilder<TKey, TValue>(config).Build();

            while (start <= end)
            {
                await ProduceMessages(producer, start);

                start++;
            }

            producer.Flush();

            return start;
        }
        
        private async Task ProduceMessages(IProducer<TKey, TValue> producer, int itValue)
        {
            foreach (var topic in Topics)
            {
                var message = GenerateMessage(itValue, topic);

                await producer.ProduceAsync(topic, message);
            }
        }

        public virtual Message<TKey, TValue> GenerateMessage(int itValue, string topic)
        {
            return new Message<TKey, TValue>();
        }
    }
}
