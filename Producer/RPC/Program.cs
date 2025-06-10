using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Text;

namespace Producer.RPC;

public class RpcClient : IAsyncDisposable
{
    private const string QueueName = "payment_gateway_queue";

    private readonly IConnectionFactory _connectionFactory;
    private readonly ConcurrentDictionary<string, TaskCompletionSource<string>> _callbackMapper = new();

    private IConnection? _connection;
    private IChannel? _channel;
    private string? _replyQueueName;

    public RpcClient()
    {
        _connectionFactory = new ConnectionFactory
        {
            HostName = "localhost", Port = 5672, UserName = "admin", Password = "admin"
        };
    }

    public async Task StartAsync()
    {
        _connection = await _connectionFactory.CreateConnectionAsync();
        _channel = await _connection.CreateChannelAsync();

        var queue = await _channel.QueueDeclareAsync();
        _replyQueueName = queue.QueueName;

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += (model, ea) =>
        {
            var correlationId = ea.BasicProperties.CorrelationId;
            if (!string.IsNullOrEmpty(correlationId) && _callbackMapper.TryRemove(correlationId, out var tcs))
            {
                var response = Encoding.UTF8.GetString(ea.Body.ToArray());
                tcs.TrySetResult(response);
            }

            return Task.CompletedTask;
        };

        await _channel.BasicConsumeAsync(_replyQueueName, true, consumer);
    }

    public async Task<string> PayAsync(string gateway, int amount, CancellationToken cancellationToken = default)
    {
        if (_channel is null) throw new InvalidOperationException();

        var correlationId = Guid.NewGuid().ToString();
        var props = new BasicProperties { CorrelationId = correlationId, ReplyTo = _replyQueueName };

        var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        _callbackMapper.TryAdd(correlationId, tcs);

        var message = $"{gateway}:{amount}";
        var body = Encoding.UTF8.GetBytes(message);

        await _channel.BasicPublishAsync(string.Empty, QueueName, true, props, body, cancellationToken);

        using var ctr = cancellationToken.Register(() =>
        {
            _callbackMapper.TryRemove(correlationId, out _);
            tcs.SetCanceled(cancellationToken);
        });

        return await tcs.Task;
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null) await _channel.CloseAsync();
        if (_connection is not null) await _connection.CloseAsync();
    }
}
