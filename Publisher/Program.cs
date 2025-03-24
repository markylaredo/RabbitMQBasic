using System.Text;
using RabbitMQ.Client;

var factory = new ConnectionFactory { HostName = "localhost" };
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

const string queueName = "task_queue";
const string dlxExchange = "dlx_exchange";

try
{
    await channel.QueueDeleteAsync(queueName);
}
catch (Exception ex)
{
    Console.WriteLine($"Queue delete failed (maybe doesn't exist): {ex.Message}");
}

// Declare the Dead Letter Exchange
await channel.ExchangeDeclareAsync(dlxExchange, ExchangeType.Fanout, durable: true);

// Declare the main queue with Dead Letter Exchange support
await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object>
    {
        { "x-dead-letter-exchange", dlxExchange } // Attach dead-letter queue
    }!
);

// Declare the Dead Letter Exchange queue and bind it
await channel.QueueDeclareAsync("dlx_queue", durable: true, exclusive: false, autoDelete: false);
await channel.QueueBindAsync("dlx_queue", dlxExchange, string.Empty);

while (true)
{
    var now = DateTime.UtcNow.AddHours(8);
    var message = $"{now:dddd MMMM yyyy HH:mm:ss.fff}";
    var body = Encoding.UTF8.GetBytes(message);

    var properties = new BasicProperties
    {
        Persistent = true,
        Headers = new Dictionary<string, object> { { "x-retry-count", 0 } }! // Initialize retry count
    };

    await channel.BasicPublishAsync(
        exchange: string.Empty,
        routingKey: queueName,
        mandatory: true,
        basicProperties: properties,
        body: body
    );

    Console.WriteLine($" [x] Sent {message}");
    await Task.Delay(1000);
}