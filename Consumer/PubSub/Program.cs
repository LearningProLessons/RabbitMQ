using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer.PubSub;

public class Program
{
    /// <summary>
    /// این متد یک مصرف‌کنندهٔ پیام راه‌اندازی می‌کند که در بستر معماری Pub/Sub و با استفاده از Exchange از نوع Fanout عمل می‌کند.
    /// در این ساختار، نیازی به تعیین نام مشخصی برای صف نیست؛ چرا که Exchange پیام‌ها را صرف‌نظر از نام یا کلید مسیر، به تمام صف‌های متصل ارسال می‌کند.
    /// صفی موقتی و ناشناس ساخته می‌شود که تنها با اتصال به نام Exchange، پیام‌های منتشرشده را دریافت و در خروجی نمایش می‌دهد.
    /// </summary>
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

        await channel.ExchangeDeclareAsync(exchange: "logs",
            type: ExchangeType.Fanout);


        QueueDeclareOk queueDeclareResult = await channel.QueueDeclareAsync();
        string queueName = queueDeclareResult.QueueName;
        await channel.QueueBindAsync(queue: queueName, exchange: "logs", routingKey: string.Empty);

        Console.WriteLine(" [*] Waiting for logs.");

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (model, ea) =>
        {
            byte[] body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            Console.WriteLine($" [x] {message}");
            return Task.CompletedTask;
        };

        await channel.BasicConsumeAsync(queueName, autoAck: true, consumer: consumer);

        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }
}