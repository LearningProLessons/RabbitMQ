using System.Text;
using RabbitMQ.Client;

namespace Producer.HelloWorld;

public class Program
{
    public Program()
    {
    }

    public async Task Run()
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
            queue: "hello",
            durable: false,
            exclusive: false,
            autoDelete: false,
            arguments: null
        );

        for (var i = 1; i <= 10000; i++)
        {
            var message = $"Message {i} sent at {DateTime.Now:HH:mm:ss}";
            var body = Encoding.UTF8.GetBytes(message);

            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: "hello",
                body: body
            );

            Console.WriteLine($" [x] Sent {message}");
            await Task.Delay(1000);
        }

        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }
}