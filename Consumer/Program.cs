// using System.Text;
// using RabbitMQ.Client;
// using RabbitMQ.Client.Events;
//
// const string queueName = "task_queue";
// const string dlxExchange = "dlx_exchange";
// const int MaxRetryCount = 3;
//
// var factory = new ConnectionFactory
// {
//     HostName = "localhost",
//     AutomaticRecoveryEnabled = true, // 🔄 Enable auto-reconnect
//     NetworkRecoveryInterval = TimeSpan.FromSeconds(5) // 🔄 Retry every 5 sec
// };
//
// async Task StartConsumerAsync()
// {
//     // while (true) // 🔄 Keep trying to reconnect
//     // {
//     try
//     {
//         using var connection = await factory.CreateConnectionAsync();
//         using var channel = await connection.CreateChannelAsync();
//
//         await channel.QueueDeclareAsync(
//             queue: queueName,
//             durable: true,
//             exclusive: false,
//             autoDelete: false,
//             arguments: new Dictionary<string, object>
//             {
//                 { "x-dead-letter-exchange", dlxExchange }
//             }
//         );
//
//         var consumer = new AsyncEventingBasicConsumer(channel);
//         consumer.ReceivedAsync += async (model, ea) =>
//         {
//             var body = ea.Body.ToArray();
//             var message = Encoding.UTF8.GetString(body);
//             var properties = ea.BasicProperties;
//
//             int retryCount = properties.Headers != null && properties.Headers.ContainsKey("x-retry-count")
//                 ? Convert.ToInt32(properties.Headers["x-retry-count"])
//                 : 0;
//
//             Console.WriteLine($" [x] Received {message}, Retry: {retryCount}/{MaxRetryCount}");
//
//             try
//             {
//                 if (new Random().Next(0, 2) == 0) // Simulate processing failure
//                 {
//                     throw new Exception("Processing failed.");
//                 }
//
//                 Console.WriteLine(" [✓] Successfully processed.");
//                 await channel.BasicAckAsync(ea.DeliveryTag, false);
//             }
//             catch (Exception ex)
//             {
//                 Console.WriteLine($" [!] Error: {ex.Message}");
//
//                 if (retryCount < MaxRetryCount)
//                 {
//                     retryCount++;
//                     Console.WriteLine($" [↻] Retrying {retryCount}/{MaxRetryCount}...");
//
//                     var newProperties = new BasicProperties
//                     {
//                         Persistent = true,
//                         Headers = new Dictionary<string, object> { { "x-retry-count", retryCount } }
//                     };
//
//                     await Task.Delay(5000);
//                     await channel.BasicPublishAsync("", queueName, true, newProperties, body);
//                 }
//                 else
//                 {
//                     Console.WriteLine(" [✗] Max retries reached. Moving to DLX.");
//                     await channel.BasicPublishAsync(dlxExchange, "", true, body);
//                 }
//
//                 await channel.BasicAckAsync(ea.DeliveryTag, false);
//             }
//         };
//
//         await channel.BasicConsumeAsync(queueName, false, consumer);
//
//         Console.WriteLine(" [*] Waiting for messages...");
//         // await Task.Delay(-1); // Keep consumer alive
//     }
//     catch (Exception ex)
//     {
//         Console.WriteLine($" [⚠] Consumer error: {ex.Message}. Reconnecting...");
//         await Task.Delay(5000); // Wait before retrying
//     }
// }
// // }
//
// // Start the consumer
// await StartConsumerAsync();


using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory
{
    HostName = "localhost",
    AutomaticRecoveryEnabled = true, // 🔄 Enable auto-reconnect
    NetworkRecoveryInterval = TimeSpan.FromSeconds(5) // 🔄 Retry every 5 sec
};
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

const string queueName = "task_queue";
const string dlxExchange = "dlx_exchange";
const int MaxRetryCount = 3;

// ✅ Ensure queue is declared with Dead Letter Exchange (DLX) (matches Publisher)
await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object>
    {
        { "x-dead-letter-exchange", dlxExchange } // Must match Publisher
    }!
);

// Declare the Dead Letter Exchange (DLX) exchange and queue
await channel.ExchangeDeclareAsync(dlxExchange, ExchangeType.Fanout, durable: true);
await channel.QueueDeclareAsync("dlx_queue",
    durable: true,
    exclusive: false,
    autoDelete: false);

await channel.QueueBindAsync("dlx_queue", dlxExchange, string.Empty);

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    var properties = ea.BasicProperties;

    int retryCount = properties.Headers != null && properties.Headers.ContainsKey("x-retry-count")
        ? Convert.ToInt32(properties.Headers["x-retry-count"])
        : 0;

    Console.WriteLine($" [x] Received {message}, Retry: {retryCount}/{MaxRetryCount}");

    try
    {
        // // Simulate processing failure
        // if (new Random().Next(0, 2) == 0)
        // {
        //     throw new Exception("Processing failed.");
        // }

        Console.WriteLine(" [✓] Successfully processed.");
        await channel.BasicAckAsync(ea.DeliveryTag, false);
    }
    catch (Exception ex)
    {
        Console.WriteLine($" [!] Error: {ex.Message}");

        if (retryCount < MaxRetryCount)
        {
            retryCount++;
            Console.WriteLine($" [↻] Retrying {retryCount}/{MaxRetryCount}...");

            var newProperties = new BasicProperties
            {
                Persistent = true,
                Headers = new Dictionary<string, object> { { "x-retry-count", retryCount } }!
            };

            await Task.Delay(5000); // Delay before retrying
            await channel.BasicPublishAsync(string.Empty, queueName, true, newProperties, body);
        }
        else
        {
            Console.WriteLine(" [✗] Max retries reached. Moving to Dead Letter Queue.");
            await channel.BasicPublishAsync(dlxExchange, string.Empty, true, body);
        }

        await channel.BasicAckAsync(ea.DeliveryTag, false);
    }
};

while (true)
{
    var ctag = await channel.BasicConsumeAsync(queueName, false, consumer);
    // Console.WriteLine(" [*] Waiting for messages... {0}", res);
    await Task.Delay(TimeSpan.FromSeconds(1));
}

Console.WriteLine(" [*] Waiting for messages...");

Console.ReadLine();