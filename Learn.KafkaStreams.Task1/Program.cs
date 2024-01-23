using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Learn.KafkaStreams.Shared;
using Learn.KafkaStreams.Task1;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System.Text.Json;

var appId = KafkaSettingsConstants.AppId("task-1");
var configuration = new Task1Configuration(appId, KafkaSettingsConstants.BootStrapServers, KafkaSettingsConstants.Task1Topic1, KafkaSettingsConstants.Task1Topic1);

SubscribeToProducerTopic();

await StartStreamAsync();

void SubscribeToProducerTopic()
{
    var config = new ConsumerConfig
    {
        GroupId = $"{configuration.ApplicationId}-result-group-id",
        BootstrapServers = configuration.BootstrapServers,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = true
    };

    var consumer = new ConsumerBuilder<Null, string>(config).Build();

    consumer.Subscribe(configuration.ConsumerTopic);

    Task.Run(() =>
    {
        while (true)
        {
            try
            {
                var consumeResult = consumer.Consume();

                try
                {
                    var message = JsonSerializer.Deserialize<Message>(consumeResult.Message.Value);

                    Console.WriteLine($"[Result Consumer] Message processed successfully - {message}");
                }
                catch
                {
                    Console.WriteLine($"[Result Consumer]Invalid format - {consumeResult.Message.Value}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Result Consumer] Error: {ex.Message}");
            }
        }
    });
}

async Task StartStreamAsync()
{
    var kafkaProducer = new Task1KafkaStreamingProducer(KafkaSettingsConstants.ProducerConfig(), configuration.ProducerTopic);

    var config = new StreamConfig<StringSerDes, StringSerDes>
    {
        ApplicationId = configuration.ApplicationId,
        BootstrapServers = configuration.BootstrapServers,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        NumStreamThreads = 3
    };

    var builder = new StreamBuilder();

    builder.Stream<string, string>(configuration.ConsumerTopic)
        .MapValues(Map)
        .To(configuration.ProducerTopic);

    using var stream = new KafkaStream(builder.Build(), config);

    await stream.StartAsync();

    var start = 1;

    while (start < 500)
    {
        start = await kafkaProducer.ProduceMessagesAsync(start, start + 15);

        await Task.Delay(5_000);
    }

    await Task.Delay(100_000);

    string Map(string json)
    {
        var message = JsonSerializer.Deserialize<Message>(json);

        Console.WriteLine($"[Stream] Mapping {json}");

        return JsonSerializer.Serialize(new Message($"[Filtered] {message.Text}"));
    }
}

#region Models
public record Message(string Text);
#endregion