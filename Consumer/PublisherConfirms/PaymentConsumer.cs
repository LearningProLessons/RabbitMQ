namespace Consumer.PublisherConfirms;

using RabbitMQ.Client;
using System.Text;
using RabbitMQ.Client.Events;

public class PaymentConsumer
{
    public static async Task RunAsync()
    {
        var factory = new ConnectionFactory
        {
            HostName = "localhost", Port = 5672, UserName = "admin", Password = "admin"
        };

        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync("payment_gateway_confirm_queue", durable: true, exclusive: false,
            autoDelete: false);
        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (sender, ea) =>
        {
            var message = Encoding.UTF8.GetString(ea.Body.ToArray());
            var parts = message.Split(':');
            var gateway = parts[0];
            var amount = parts[1];

            var trackingCode = Guid.NewGuid().ToString("N")[..6];
            Console.WriteLine($"✔ پرداخت {amount} تومان از طریق {gateway} انجام شد | کد: {trackingCode}");

            await channel.BasicAckAsync(ea.DeliveryTag, false);
        };

        await channel.BasicConsumeAsync("payment_gateway_confirm_queue", autoAck: false, consumer);
        Console.WriteLine("🟢 Consumer آماده‌ست");
        Console.ReadLine();
    }
}
