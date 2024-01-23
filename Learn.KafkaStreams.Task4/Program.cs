using Confluent.Kafka.Admin;
using Confluent.Kafka;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net;
using Newtonsoft.Json;
using Learn.KafkaStreams.Shared;
using Learn.KafkaStreams.Task4;

var appId = KafkaSettingsConstants.AppId("task4");

var configuration = new Configuration(appId, KafkaSettingsConstants.BootStrapServers);

await StartStreamAsync();

async Task StartStreamAsync()
{
    var config = new StreamConfig<StringSerDes, Learn.KafkaStreams.Shared.JsonSerDes<Employee>>
    {
        ApplicationId = configuration.ApplicationId,
        BootstrapServers = configuration.BootstrapServers,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        NumStreamThreads = 3
    };

    var topology = new TopologyBuilder()
        .WithResultHandler(Print)
        .Build(KafkaSettingsConstants.Task4Topic1);

    using var stream = new KafkaStream(topology, config);

    await stream.StartAsync();

    var start = 1;

    while (start < 50)
    {
        start = await GenerateConsumerTopicMessagesAsync(start, start + 15);

        await Task.Delay(10_000);
    }

    await Task.Delay(100_000);

    void Print(string key, Employee value)
    {
        Console.WriteLine(value);
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
        var employee = new Employee($"Company-{Guid.NewGuid()}", $"Employee-{Guid.NewGuid()}-Name", $"Position#{start}", (short)start);

        var message = new Message<Null, string>
        {
            Value = JsonConvert.SerializeObject(employee)
        };

        await producer.ProduceAsync(KafkaSettingsConstants.Task4Topic1, message);

        start++;
    }

    producer.Flush();

    return start;
}

#region Models
public record Employee(string Name, string Company, string Position, short Experience);
#endregion
