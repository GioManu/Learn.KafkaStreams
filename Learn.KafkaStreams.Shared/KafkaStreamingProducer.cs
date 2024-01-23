using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Learn.KafkaStreams.Shared
{
    public class KafkaStreamingProducer(ProducerConfig config, string TopicName)
    {
        public async Task<int> ProduceMessagesAsync(int start, int end)
        {
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            while (start <= end)
            {
                var message = new Message<Null, string>
                {
                    Value = JsonConvert.SerializeObject(new Message($"auto generated message #{start}"))
                };

                await producer.ProduceAsync(TopicName, message);

                start++;
            }

            producer.Flush();

            return start;
        }
    }
}
