using Confluent.Kafka;
using Newtonsoft.Json;
using System.Globalization;

var config = new ConsumerConfig
{
    GroupId = "weather-consumer-group",
    BootstrapServers = "localhost:9092",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new ConsumerBuilder<Null, string>(config).Build();
consumer.Subscribe("five");
CancellationToken token = new();


try
{
    while (true)
    {
        var response = consumer.Consume(token);
        if (response.Message != null)
        {
            string timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff",
                CultureInfo.InvariantCulture);
            Console.WriteLine(timestamp);            var weather = JsonConvert.DeserializeObject<Weather>(response.Message.Value);
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
