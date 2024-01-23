using Confluent.Kafka.Admin;
using Confluent.Kafka;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net;
using Learn.KafkaStreams.Shared;
using Learn.KafkaStreams.Task2;

var appId = KafkaSettingsConstants.AppId("task2");
var configuration = new Task2Configuration(appId, KafkaSettingsConstants.BootStrapServers, KafkaSettingsConstants.Task2Topic1);

await StartStreamAsync();

async Task StartStreamAsync()
{
    var kafkaStreamingProducer = new KafkaStreamingProducer(KafkaSettingsConstants.ProducerConfig(), configuration.Topic);

    var config = new StreamConfig<StringSerDes, StringSerDes>
    {
        ApplicationId = configuration.ApplicationId,
        BootstrapServers = configuration.BootstrapServers,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        NumStreamThreads = 3
    };

    var topology = new TopologyBuilder()
        .WithIntermediateResultHandler(PrintWord)
        .WithResultHandler(PrintFinalWord)
        .Build(configuration.Topic);

    using var stream = new KafkaStream(topology, config);

    await stream.StartAsync();

    var start = 1;

    while (start < 500)
    {
        start = await kafkaStreamingProducer.ProduceMessagesAsync(start, start + 15);

        await Task.Delay(5_000);
    }

    await Task.Delay(100_000);

    void PrintWord(int key, string value)
    {
        Console.WriteLine($"Word - Length {key} - value {value}");
    }

    void PrintFinalWord(int key, string value)
    {
        if (key < 10)
        {
            Console.WriteLine($"Short Word - Length {key} - value {value}");
        }
        else
        {
            Console.WriteLine($"Long Word - Length {key} - value {value}");
        }

    }
}