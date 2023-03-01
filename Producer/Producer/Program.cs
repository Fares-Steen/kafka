using Confluent.Kafka;
using Newtonsoft.Json;
using System.Globalization;
using System.Text.Json.Serialization;
using Random = Producer.Random;

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
        for (int i = 0; i < 1; i++)
        {
            Task.Run(async() =>
            {
                var response2 = await producer.ProduceAsync("five",
                    new Message<Null, string> { Value = JsonConvert.SerializeObject(new Weather(Random.RandomString(300),i)) });
                string timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff",
                    CultureInfo.InvariantCulture);
                Console.WriteLine(timestamp);
                Console.WriteLine(response2.Message.Value);
            });
            // var response2 = await producer.ProduceAsync("five",
            //         new Message<Null, string> { Value = JsonConvert.SerializeObject(new Weather(state,i)) });
            //     Console.WriteLine(response2.Message.Value);
        }
        // var topicPart = new TopicPartition("weather-topic", new Partition(3)); 
        //
        // var response = await producer.ProduceAsync("five",
        //     new Message<Null, string> { Value = JsonConvert.SerializeObject(new Weather(state,90)) });
        //
        // Console.WriteLine(response.Message.Value);
    }

}
catch (Exception e)
{
    Console.WriteLine(e);
    throw;
}


public record Weather(string State, int Temperature); 
//kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 3 --topic nameOfTopic