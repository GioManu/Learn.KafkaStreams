using Confluent.Kafka.Admin;
using Confluent.Kafka;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Stream;
using Learn.KafkaStreams.Shared;
using Learn.KafkaStreams.Task3;

var appId = KafkaSettingsConstants.AppId("task3");
var topics = new string[] { KafkaSettingsConstants.Task3Topic1, KafkaSettingsConstants.Task3Topic2 };
var configuration = new Task3Configuration(appId, KafkaSettingsConstants.BootStrapServers, topics);

await StartStreamAsync();

async Task StartStreamAsync()
{
    var config = new StreamConfig<StringSerDes, StringSerDes>
    {
        ApplicationId = configuration.ApplicationId,
        BootstrapServers = configuration.BootstrapServers,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        NumStreamThreads = 3
    };

    var builder = new StreamBuilder();

    var innerStreams = configuration.Topics.Select(topicName => builder
                .Stream<string, string>(topicName)
                .Filter(Filter)
                .Map(CreateKey)
                .Peek(Print));

    var innerStream = innerStreams.First();
    var valueSerDes = new StringSerDes();
    var keySerDes = new Int64SerDes();

    foreach (var nextInnerStream in innerStreams.Skip(1))
    {
        innerStream = innerStream.Join(nextInnerStream, Join, JoinWindowOptions.Of(TimeSpan.FromMinutes(1)), StreamJoinProps.With(keySerDes, valueSerDes, valueSerDes));
    }

    innerStream.Foreach(PrintJoin);

    using var stream = new KafkaStream(builder.Build(), config);

    await stream.StartAsync();

    var start = 1;

    while (start < 50)
    {
        start = await GenerateConsumerTopicMessagesAsync(start, start + 15);

        await Task.Delay(10_000);
    }

    await Task.Delay(100_000);

    bool Filter(string key, string value)
    {
        return value is not null && value.Contains(':');
    }

    KeyValuePair<long, string> CreateKey(string key, string value)
    {
        var words = value.Split(':', count: 2);

        return KeyValuePair.Create(long.Parse(words.First()), words.Last());
    }

    void Print(long key, string value)
    {
        Console.WriteLine($"Key: {key} - Value: {value}");
    }

    string Join(string left, string right)
    {
        return $"{left} - {right}";
    }

    void PrintJoin(long key, string value)
    {
        Console.WriteLine($"Key: {key} - Value: {value}");
    }
}

async Task<int> GenerateConsumerTopicMessagesAsync(int start, int end)
{
    var config = new ProducerConfig
    {
        BootstrapServers = configuration.BootstrapServers,
        Acks = Acks.All,
        EnableIdempotence = true,
        MessageSendMaxRetries = int.MaxValue,
        MessageTimeoutMs = 10_000
    };

    using var producer = new ProducerBuilder<Null, string>(config).Build();

    while (start <= end)
    {
        foreach (var topicName in configuration.Topics)
        {
            var message = new Message<Null, string>
            {
                Value = $"{start}:{topicName}'s value"
            };

            await producer.ProduceAsync(topicName, message);
        }

        start++;
    }

    producer.Flush();

    return start;
}
