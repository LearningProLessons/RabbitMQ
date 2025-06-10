using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Text;

namespace Producer.RPC;

public class RpcClient : IAsyncDisposable
{
    private const string QueueName = "rpc_queue";

    private readonly IConnectionFactory _connectionFactory;

    private readonly ConcurrentDictionary<string, TaskCompletionSource<string>> _callbackMapper
        = new();

    private IConnection? _connection;
    private IChannel? _channel;
    private string? _replyQueueName;

    public RpcClient()
    {
        _connectionFactory = new ConnectionFactory
        {
            HostName = "localhost", Port = 5672, UserName = "admin", Password = "admin"
        };
        ;
    }

    public async Task StartAsync()
    {
        _connection = await _connectionFactory.CreateConnectionAsync();
        _channel = await _connection.CreateChannelAsync();

        // declare a server-named queue
        QueueDeclareOk queueDeclareResult = await _channel.QueueDeclareAsync();
        _replyQueueName = queueDeclareResult.QueueName;
        var consumer = new AsyncEventingBasicConsumer(_channel);

        consumer.ReceivedAsync += (model, ea) =>
        {
            string? correlationId = ea.BasicProperties.CorrelationId;

            if (false == string.IsNullOrEmpty(correlationId))
            {
                if (_callbackMapper.TryRemove(correlationId, out var tcs))
                {
                    var body = ea.Body.ToArray();
                    var response = Encoding.UTF8.GetString(body);
                    tcs.TrySetResult(response);
                }
            }

            return Task.CompletedTask;
        };

        await _channel.BasicConsumeAsync(_replyQueueName, true, consumer);
    }

    public async Task<string> CallAsync(string message,
        CancellationToken cancellationToken = default)
    {
        if (_channel is null)
        {
            throw new InvalidOperationException();
        }

        string correlationId = Guid.NewGuid().ToString();
        var props = new BasicProperties { CorrelationId = correlationId, ReplyTo = _replyQueueName };

        var tcs = new TaskCompletionSource<string>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        _callbackMapper.TryAdd(correlationId, tcs);

        var messageBytes = Encoding.UTF8.GetBytes(message);
        await _channel.BasicPublishAsync(exchange: string.Empty, routingKey: QueueName,
            mandatory: true, basicProperties: props, body: messageBytes, cancellationToken: cancellationToken);

        using CancellationTokenRegistration ctr =
            cancellationToken.Register(() =>
            {
                _callbackMapper.TryRemove(correlationId, out _);
                tcs.SetCanceled(cancellationToken);
            });

        return await tcs.Task;
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
        {
            await _channel.CloseAsync();
        }

        if (_connection is not null)
        {
            await _connection.CloseAsync();
        }
    }
}
