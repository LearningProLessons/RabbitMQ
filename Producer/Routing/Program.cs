using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

namespace Producer.Routing;

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
        var selectedGateway = "idpay";

        var paymentRequest = new PaymentRequest
        {
            OrderId = Guid.NewGuid().ToString(),
            Amount = 150000
        };

        var json = JsonSerializer.Serialize(paymentRequest);
        var body = Encoding.UTF8.GetBytes(json);

        await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Direct, durable: true);

        await channel.BasicPublishAsync(
            exchange: exchangeName,
            routingKey: selectedGateway,
            body: body
        );


        Console.WriteLine(
            $"Published payment for {selectedGateway} → OrderId: {paymentRequest.OrderId}, Amount: {paymentRequest.Amount}");
    }
}

public class PaymentRequest
{
    public string OrderId { get; set; }
    public int Amount { get; set; }
}