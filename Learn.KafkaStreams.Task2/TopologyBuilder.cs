using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net;

namespace Learn.KafkaStreams.Task2
{
    public class TopologyBuilder
    {
        private Action<int, string>? intermediateResultHandler;
        private Action<int, string>? resultHandler;

        public TopologyBuilder WithIntermediateResultHandler(Action<int, string> handler)
        {
            intermediateResultHandler = handler;
            return this;
        }

        public TopologyBuilder WithResultHandler(Action<int, string> handler)
        {
            resultHandler = handler;
            return this;
        }

        public Topology Build(string topicName)
        {
            var builder = new StreamBuilder();

            var branches = builder
                .Stream<string, string>(topicName)
                .FilterNot(IsNull)
                .FlatMap(Split)
                .Peek(HandleIntermediateResult)
                .Branch(IsShort, (k, v) => true);

            for (var i = 0; i < branches.Length; i++)
            {
                branches[i] = branches[i].FilterNot((k, v) => Contains(v, 'a'));
            }

            branches
                .First()
                .Merge(branches.Last())
                .Foreach(HandleResult);

            return builder.Build();
        }

        private static bool IsNull(string key, string value)
        {
            return value is null;
        }

        private static IEnumerable<KeyValuePair<int, string>> Split(string key, string value)
        {
            var words = value.Split(' ', StringSplitOptions.RemoveEmptyEntries);

            if (words.Length == 1)
            {
                yield return KeyValuePair.Create(value.Length, value);
                yield break;
            }

            foreach (var word in words)
            {
                yield return KeyValuePair.Create(word.Length, word);
            }
        }

        private static bool IsShort(int key, string word)
        {
            return key < 10;
        }

        private static bool Contains(string word, char ch)
        {
            return word.Contains(ch, StringComparison.CurrentCultureIgnoreCase);
        }

        private void HandleIntermediateResult(int key, string value)
        {
            intermediateResultHandler?.Invoke(key, value);
        }

        private void HandleResult(int key, string value)
        {
            resultHandler?.Invoke(key, value);
        }
    }
}
