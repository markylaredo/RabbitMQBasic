using RabbitMQ.Client;
using System.Text;

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync(queue: "task_queue", durable: true, exclusive: false,
    autoDelete: false, arguments: null);


while (true)
{
    var now = DateTime.UtcNow.AddHours(8);
    var message = $"{now:dddd MMMM yyyy HH:mm:ss.fff}";
    var body = Encoding.UTF8.GetBytes(message);

    var properties = new BasicProperties
    {
        Persistent = true
    };

    await channel.BasicPublishAsync(exchange: string.Empty, routingKey: "task_queue", mandatory: true,
        basicProperties: properties, body: body);

    Console.WriteLine($" [x] Sent {message}");
    await Task.Delay(1000);
}