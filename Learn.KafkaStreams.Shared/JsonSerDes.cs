using Confluent.Kafka;
using Newtonsoft.Json;
using Streamiz.Kafka.Net.SerDes;
using System.Text;

namespace Learn.KafkaStreams.Shared
{
    public class JsonSerDes<T> : AbstractSerDes<T> where T : class
    {
        private readonly Encoding encoding;

        public JsonSerDes()
        {
            encoding = Encoding.UTF8;
        }

        public override T Deserialize(byte[] data, SerializationContext context)
        {
            if (data is null)
            {
                return null;
            }

            return JsonConvert.DeserializeObject<T>(encoding.GetString(data));
        }

        public override byte[] Serialize(T data, SerializationContext context)
        {
            if (data is null)
            {
                return null;
            }

            return encoding.GetBytes(JsonConvert.SerializeObject(data));
        }
    }
}
