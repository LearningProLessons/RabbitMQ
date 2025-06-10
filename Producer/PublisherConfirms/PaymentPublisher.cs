namespace Producer.PublisherConfirms;

using System.Text;
using RabbitMQ.Client;


public class PaymentPublisher : IAsyncDisposable
{
    private IConnection? _connection;
    private IChannel? _channel;

    private readonly LinkedList<ulong> _outstandingConfirms = new();
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private TaskCompletionSource<bool>? _allConfirmed;

    public async Task<bool> PublishAsync(string gateway, int amount)
    {
        var factory = new ConnectionFactory
        {
            HostName = "localhost", Port = 5672, UserName = "admin", Password = "admin"
        };

        _connection = await factory.CreateConnectionAsync();

        var channelOpts = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true,
            outstandingPublisherConfirmationsRateLimiter: new ThrottlingRateLimiter(2)
        );

        _channel = await _connection.CreateChannelAsync(channelOpts, CancellationToken.None);

        await _channel.QueueDeclareAsync("payment_gateway_confirm_queue", durable: true, exclusive: false, autoDelete: false);

        _allConfirmed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

       
        _channel.BasicAcksAsync += async (o, ea) =>
        {
            o = Task.Run(() => CleanOutstandingConfirms(ea.DeliveryTag, ea.Multiple));
        };

        _channel.BasicNacksAsync += async (o, ea) =>
        {
            o = Task.Run(() => CleanOutstandingConfirms(ea.DeliveryTag, ea.Multiple));
        };

        
        var message = $"{gateway}:{amount}";
        var body = Encoding.UTF8.GetBytes(message);

        var seqNo = await _channel.GetNextPublishSequenceNumberAsync();
        _outstandingConfirms.AddLast(seqNo);

        await _channel.BasicPublishAsync(exchange: string.Empty, routingKey: "payment_gateway_confirm_queue", body: body);

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await using (timeout.Token.Register(() => _allConfirmed.TrySetResult(false)))
        {
            return await _allConfirmed.Task;
        }
    }

    private async Task CleanOutstandingConfirms(ulong deliveryTag, bool multiple)
    {
        await _semaphore.WaitAsync();
        try
        {
            if (multiple)
            {
                while (_outstandingConfirms.First is { } node && node.Value <= deliveryTag)
                {
                    _outstandingConfirms.RemoveFirst();
                }
            }
            else
            {
                _outstandingConfirms.Remove(deliveryTag);
            }

            if (_outstandingConfirms.Count == 0)
            {
                _allConfirmed?.TrySetResult(true);
            }
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
            await _channel.CloseAsync();

        if (_connection is not null)
            await _connection.CloseAsync();
    }
}

