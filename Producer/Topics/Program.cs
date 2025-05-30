using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

namespace Producer.Topics;

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

        var exchangeName = "event-topic-exchange";
        await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Topic, durable: true);

        var events = new List<(string RoutingKey, object Payload)>
        {
            ("payment.created", new { OrderId = "X001", Amount = 100000 }),
            ("payment.failed", new { OrderId = "X002", Reason = "Card Declined" }),
            ("refund.created", new { OrderId = "X004", Amount = 50000 }),
            ("refund.failed", new { OrderId = "X003", Reason = "Refund Window Expired" }),
            ("payment.refunded", new { OrderId = "X005", Amount = 100000 }),
            ("payment.pending", new { OrderId = "X006", Amount = 75000 }),
            ("account.update", new { AccountId = "A100", Status = "Active" }),
            ("payment.error.critical", new { OrderId = "X007", ErrorCode = "E999", Message = "System Outage" }),
            ("payment.error.minor", new { OrderId = "X008", ErrorCode = "E101", Message = "Timeout" }),
            ("refund.error", new { OrderId = "X009", ErrorCode = "E201", Message = "Invalid Refund" })
        };


        foreach (var (routingKey, payload) in events)
        {
            var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(payload));
            await channel.BasicPublishAsync(exchange: exchangeName, routingKey: routingKey, body: body);
            
            Console.WriteLine($"[PUBLISH] '{routingKey}':'{payload}'");
        }
    }
}