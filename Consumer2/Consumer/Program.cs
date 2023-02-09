using Confluent.Kafka;
using Newtonsoft.Json;

var config = new ConsumerConfig
{
    GroupId = "weather-consumer-group",
    BootstrapServers = "localhost:9092",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new ConsumerBuilder<Null, string>(config).Build();
consumer.Subscribe("four");
CancellationToken token = new();

try
{
    while (true)
    {
        var response = consumer.Consume(token);
        if (response.Message != null)
        {
            var weather = JsonConvert.DeserializeObject<Weather>(response.Message.Value);
            Console.WriteLine($"{weather!.State} temperature {weather.Temperature}");
        }
    }
}
catch (Exception e)
{
    Console.WriteLine(e.Message);
    throw;
}
public record Weather(string State, int Temperature); 
