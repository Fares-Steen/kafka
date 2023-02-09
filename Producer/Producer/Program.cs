using Confluent.Kafka;
using Newtonsoft.Json;
using System.Text.Json.Serialization;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    Partitioner = Partitioner.Random
};
using var producer = new ProducerBuilder<Null, string>(config).Build();
try
{
    string? state;
    while ((state = Console.ReadLine() )!=null)
    {
        // for (int i = 0; i < 500; i++)
        // {
        //     var response2 = await producer.ProduceAsync("weather-topic",
        //         new Message<Null, string> { Value = JsonConvert.SerializeObject(new Weather(state,i)) });
        //     Console.WriteLine(response2.Message.Value);
        //
        // }
        var topicPart = new TopicPartition("weather-topic", new Partition(3)); 

        var response = await producer.ProduceAsync("four",
            new Message<Null, string> { Value = JsonConvert.SerializeObject(new Weather(state,90)) });

        Console.WriteLine(response.Message.Value);
    }

}
catch (Exception e)
{
    Console.WriteLine(e);
    throw;
}

public record Weather(string State, int Temperature); 
