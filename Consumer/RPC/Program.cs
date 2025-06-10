using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace Consumer.RPC;

public class Program
{
    public static async Task RunAsync()
    {
        const string QUEUE_NAME = "payment_gateway_queue";

        var factory = new ConnectionFactory
        {
            HostName = "localhost", Port = 5672, UserName = "admin", Password = "admin"
        };
        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(queue: QUEUE_NAME, durable: false, exclusive: false,
            autoDelete: false, arguments: null);

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (sender, ea) =>
        {
            var ch = ((AsyncEventingBasicConsumer)sender).Channel;
            var props = ea.BasicProperties;
            var replyProps = new BasicProperties { CorrelationId = props.CorrelationId };

            var request = Encoding.UTF8.GetString(ea.Body.ToArray());
            var response = await ProcessPaymentAsync(request);

            var responseBytes = Encoding.UTF8.GetBytes(response);
            await ch.BasicPublishAsync(string.Empty, props.ReplyTo!, true, replyProps, responseBytes);
            await ch.BasicAckAsync(ea.DeliveryTag, false);
        };

        await channel.BasicConsumeAsync(QUEUE_NAME, false, consumer);
        Console.WriteLine(" [x] Awaiting payment requests");
        Console.ReadLine();
    }

    static Task<string> ProcessPaymentAsync(string request)
    {
        var parts = request.Split(':');
        var gateway = parts[0];
        var amount = parts[1];

        var trackingCode = Guid.NewGuid().ToString("N")[..6];
        return Task.FromResult($"پرداخت {amount} تومان از طریق {gateway} انجام شد | کد پیگیری: {trackingCode}");
    }
}
