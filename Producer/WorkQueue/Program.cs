using RabbitMQ.Client;
using System.Text;

namespace Producer.WorkQueue;

public class Program
{
    public static async Task Main(string[] args)
    {
        await RunAsync(args);
    }

    public static async Task RunAsync(string[] args)
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

        await channel.QueueDeclareAsync(queue: "task_queue", durable: true, exclusive: false,
            autoDelete: false, arguments: null);

        var message = GetMessage(args);
        var body = Encoding.UTF8.GetBytes(message);

        var properties = new BasicProperties
        {
            Persistent = true
        };

        await channel.BasicPublishAsync(exchange: string.Empty, routingKey: "task_queue", mandatory: true,
            basicProperties: properties, body: body);
        Console.WriteLine($" [x] Sent {message}");

        static string GetMessage(string[] args)
        {
            return ((args.Length > 0) ? string.Join(" ", args) : "Hello World!");
        }
    }
}