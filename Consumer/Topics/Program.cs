using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer.Topics;

public class Program
{
    public static async Task RunAsync()
    {
        var loggerTask = LoggerConsumerAsync();
        var accountingTask = AccountingConsumerAsync();
        var errorHandlerTask = ErrorHandlerConsumerAsync();

        await Task.WhenAll(loggerTask, accountingTask, errorHandlerTask);
    }

    private static async Task LoggerConsumerAsync()
    {
        var factory = new ConnectionFactory
            { HostName = "localhost", Port = 5672, UserName = "admin", Password = "admin" };
        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        var exchangeName = "event-topic-exchange";
        var queueName = "logger-queue";

        await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Topic, durable: true);
        await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false);
        await channel.QueueBindAsync(queueName, exchangeName, "#");

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (model, ea) =>
        {
            var message = Encoding.UTF8.GetString(ea.Body.ToArray());
            Console.WriteLine($"[LOGGER] '{ea.RoutingKey}':'{message}'");
            await Task.CompletedTask;
        };

        await channel.BasicConsumeAsync(queueName, autoAck: true, consumer: consumer);

        await Task.Delay(-1);
    }

    private static async Task AccountingConsumerAsync()
    {
        var factory = new ConnectionFactory
            { HostName = "localhost", Port = 5672, UserName = "admin", Password = "admin" };
        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        var exchangeName = "event-topic-exchange";
        var queueName = "accounting-queue";

        await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Topic, durable: true);
        await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false);
        await channel.QueueBindAsync(queueName, exchangeName, "payment.*");

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (model, ea) =>
        {
            var message = Encoding.UTF8.GetString(ea.Body.ToArray());
            Console.WriteLine($"[ACCOUNTING] '{ea.RoutingKey}':'{message}'");
            await Task.CompletedTask;
        };

        await channel.BasicConsumeAsync(queueName, autoAck: true, consumer: consumer);

        await Task.Delay(-1);
    }

    private static async Task ErrorHandlerConsumerAsync()
    {
        var factory = new ConnectionFactory
            { HostName = "localhost", Port = 5672, UserName = "admin", Password = "admin" };
        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        var exchangeName = "event-topic-exchange";
        var queueName = "errors-queue";

        await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Topic, durable: true);
        await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false);
        await channel.QueueBindAsync(queueName, exchangeName, "*.failed");

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (model, ea) =>
        {
            var message = Encoding.UTF8.GetString(ea.Body.ToArray());
            Console.WriteLine($"[ERROR-HANDLER] '{ea.RoutingKey}':'{message}'");
            await Task.CompletedTask;
        };

        await channel.BasicConsumeAsync(queueName, autoAck: true, consumer: consumer);

        await Task.Delay(-1);
    }
}