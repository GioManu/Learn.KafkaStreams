using Confluent.Kafka;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net;
using Learn.KafkaStreams.Shared;
using Learn.KafkaStreams.Task4;

var appId = KafkaSettingsConstants.AppId("task4");

var configuration = new Task4Configuration(appId, KafkaSettingsConstants.BootStrapServers, KafkaSettingsConstants.Task4Topic1);

await StartStreamAsync();

async Task StartStreamAsync()
{
    var kafkaProducer = new Task4KafkaStreamingProducer(KafkaSettingsConstants.ProducerConfig(), configuration.Topic);

    var config = new StreamConfig<StringSerDes, Learn.KafkaStreams.Shared.JsonConvertor<Employee>>
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
        start = await kafkaProducer.ProduceMessagesAsync(start, start + 15);

        await Task.Delay(10_000);
    }

    await Task.Delay(100_000);

    void Print(string key, Employee value)
    {
        Console.WriteLine(value);
    }
}

#region Models
public record Employee(string Name, string Company, string Position, short Experience);
#endregion
