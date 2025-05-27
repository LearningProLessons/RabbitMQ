using System.Text;
using RabbitMQ.Client;

namespace Producer.WorkQueue;

public class Program
{
    public static async Task RunAsync()
    {
        var factory = new ConnectionFactory
        {
            HostName = "localhost",
            Port = 5672,
            UserName = "admin",
            Password = "admin"
        };

        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(
            queue: "work-queue",
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null);


        for (int i = 1; i <= 10; i++)
        {
            var message = $"Task #{i}";
            var body = Encoding.UTF8.GetBytes(message);

            var properties = new BasicProperties();
            properties.Persistent = true;


            await channel.BasicPublishAsync(
                exchange: "",
                routingKey: "work-queue",
                mandatory: false,
                basicProperties: properties,
                body: body);


            Console.WriteLine($" [x] Sent {message}");
        }
    }
}