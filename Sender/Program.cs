using System;
using System.Threading.Tasks;
using NServiceBus;

class Program
{
    static async Task Main()
    {
        Console.Title = "Samples.RabbitMQ.SimpleSender";
        #region ConfigureRabbit
        var endpointConfiguration = new EndpointConfiguration("Samples.RabbitMQ.SimpleSender");
        var transport = endpointConfiguration.UseTransport<RabbitMQTransport>();
        transport.UseConventionalRoutingTopology();
        transport.ConnectionString("host=localhost; user=guest; password=guest;");
        #endregion

        transport.Routing().RouteToEndpoint(typeof(MyCommand), "Samples.RabbitMQ.SimpleReceiver");
        endpointConfiguration.EnableInstallers();

        var endpointInstance = await Endpoint.Start(endpointConfiguration)
            .ConfigureAwait(false);
        await SendMessages(endpointInstance);
        await endpointInstance.Stop()
            .ConfigureAwait(false);
    }

    static async Task SendMessages(IMessageSession messageSession)
    {
        while (true)
        {
            await messageSession.Send(new MyCommand()).ConfigureAwait(false);
            await Task.Delay(TimeSpan.FromMinutes(1)).ConfigureAwait(false);
        }
    }
}