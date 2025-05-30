using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer.Routing;

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

        var exchangeName = "payment-gateway-routing-exchange";
        var queueName = "idpay-queue";
        var routingKey = "idpay";

        await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Direct, durable: true);
        await channel.QueueDeclareAsync(queue: queueName, durable: true, exclusive: false, autoDelete: false);
        await channel.QueueBindAsync(queue: queueName, exchange: exchangeName, routingKey: routingKey);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (sender, ea) =>
        {
            var json = Encoding.UTF8.GetString(ea.Body.ToArray());
            var payment = JsonSerializer.Deserialize<PaymentRequest>(json);

            Console.WriteLine(
                $"[IDPay Consumer] Received Payment → OrderId: {payment?.OrderId}, Amount: {payment?.Amount}");

            await Task.CompletedTask;
        };

        await channel.BasicConsumeAsync(queue: queueName, autoAck: true, consumer: consumer);

        Console.WriteLine("IDPay Consumer is running. Press [Enter] to exit.");
        Console.ReadLine();
    }
}

public class PaymentRequest
{
    public string OrderId { get; set; }
    public int Amount { get; set; }
}