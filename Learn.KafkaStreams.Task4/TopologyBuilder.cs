using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net;

namespace Learn.KafkaStreams.Task4
{
    public class TopologyBuilder
    {
        private Action<string, Employee> resultHandler;

        public TopologyBuilder WithResultHandler(Action<string, Employee> resultHandler)
        {
            this.resultHandler = resultHandler;
            return this;
        }

        public Topology Build(string topicName)
        {
            var builder = new StreamBuilder();

            builder
                .Stream<string, Employee>(topicName)
                .Filter(Filter)
                .Foreach(ProcessResult);

            return builder.Build();
        }

        private bool Filter(string key, Employee value)
        {
            return value is not null;
        }

        private void ProcessResult(string key, Employee value)
        {
            resultHandler?.Invoke(key, value);
        }
    }
}
