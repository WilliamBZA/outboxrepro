using System;

namespace Receiver
{
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using Autofac;
    using Microsoft.Data.SqlClient;
    using NServiceBus;

    public class Program // : ServiceBase
    {
        private static IContainer _autofac;
        private static IEndpointInstance endpointInstance;
        private static object SyncRoot = new object();

        private static bool IsRestarting { get; set; }
        private static bool IsRunning { get; set; }
        private static int RestartAttempts { get; set; }

        static async Task Main()
        {
            Console.Title = "Samples.RabbitMQ.SimpleReceiver";

            InitializeContainer();
            await StartEndpoint().ConfigureAwait(false);
            await ProcessInput().ConfigureAwait(false);

            //using (var service = new Program())
            //{
            //    try
            //    {
            //        Console.CancelKeyPress += (sender, e) =>
            //        {
            //            Console.WriteLine("Stopping service....");
            //            service.OnStop();
            //            Environment.Exit(0);
            //        };

            //        service.OnStart(null);

            //        await ProcessInput().ConfigureAwait(false);

            //    }
            //    catch (Exception ex)
            //    {
            //        Console.Write($"Error: {ex.Message}");
            //        service.OnStop();
            //    }
            //}


        }

        static async Task StartEndpoint()
        {
            var endpointConfiguration = new EndpointConfiguration("Samples.RabbitMQ.SimpleReceiver");

#pragma warning disable CS0618 // Type or member is obsolete
            endpointConfiguration.UseContainer<AutofacBuilder>(
#pragma warning restore CS0618 // Type or member is obsolete
                    customizations =>
                    {
                        customizations.ExistingLifetimeScope(_autofac);
                    });

            var transport = endpointConfiguration.UseTransport<RabbitMQTransport>();
            transport.UseConventionalRoutingTopology();
            transport.ConnectionString("host=localhost; username=guest; password=guest;");
            endpointConfiguration.EnableInstallers();
            endpointConfiguration.SendHeartbeatTo("Particular.Servicecontrol", TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(15));

            var connection = @"Data Source=.,1433;Initial Catalog=OutboxRepro;User ID=sa;Password=Asdf1234!;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False";
            var tablePrefix = "Persistence_";

            var persistence = endpointConfiguration.UsePersistence<SqlPersistence>();
            persistence.TablePrefix(tablePrefix);

            var subscriptions = persistence.SubscriptionSettings();
            subscriptions.CacheFor(TimeSpan.FromMinutes(1));

            persistence.SqlDialect<SqlDialect.MsSqlServer>();
            persistence.ConnectionBuilder(() => new SqlConnection(connection));

            endpointConfiguration.MakeInstanceUniquelyAddressable($"Callbacks-{Environment.MachineName}");
            endpointConfiguration.EnableCallbacks();

            var outboxSettings = endpointConfiguration.EnableOutbox();
            outboxSettings.RunDeduplicationDataCleanupEvery(TimeSpan.FromSeconds(10));

            EnableUnitOfWork(endpointConfiguration);

            endpointConfiguration.DefineCriticalErrorAction(OnCriticalError);

            endpointInstance = await Endpoint.Start(endpointConfiguration).ConfigureAwait(false);

            IsRunning = true;
        }

        static void EnableUnitOfWork(EndpointConfiguration endpointConfiguration)
        {
            endpointConfiguration.RegisterComponents(registration: components =>
            {
                components.ConfigureComponent<CustomUnitOfWork>(DependencyLifecycle.InstancePerUnitOfWork);
            });
            var unitOfWork = endpointConfiguration.UnitOfWork();
            unitOfWork.WrapHandlersInATransactionScope();
        }

        static async Task ProcessInput()
        {
            Console.WriteLine("Press [r] to stop RabbitMQ for 11 minutes, or [s] to stop SQL for 11 minutes. Press [Esc] to exit.");
            while (true)
            {
                var input = Console.ReadKey();
                Console.WriteLine();

                switch (input.Key)
                {
                    case ConsoleKey.R:
                        await StopRabbitMQ().ConfigureAwait(false);
                        break;
                    case ConsoleKey.S:
                        await StopSqlServer().ConfigureAwait(false);
                        break;
                    case ConsoleKey.Escape:
                        return;
                }
            }
        }

        static async Task StopSqlServer()
        {
            var stopSql = new ProcessStartInfo("docker", "stop sql");

            Console.WriteLine("Stopping Sql Server");
            Process.Start(stopSql).WaitForExit();
            Console.WriteLine("Sql stopped. Waiting 11 minutes.");

            await Task.Delay(TimeSpan.FromMinutes(11)).ConfigureAwait(false);

            var startSql = new ProcessStartInfo("docker", "start sql");

            Console.WriteLine("Starting Sql Server");
            Process.Start(startSql).WaitForExit();
            Console.WriteLine("Sql started.");
        }

        static async Task StopRabbitMQ()
        {
            var stopRabbit = new ProcessStartInfo("docker", "stop rabbit");

            Console.WriteLine("Stopping RabbitMQ");
            Process.Start(stopRabbit).WaitForExit();
            Console.WriteLine("RabbitMQ stopped. Waiting 11 minutes.");

            await Task.Delay(TimeSpan.FromMinutes(11)).ConfigureAwait(false);

            var startRabbit = new ProcessStartInfo("docker", "start rabbit");

            Console.WriteLine("Starting RabbitMQ");
            Process.Start(startRabbit).WaitForExit();
            Console.WriteLine("RabbitMQ started.");
        }

        static void PreLoadAllAssemblies()
        {
            var assemblies = Directory.GetFiles(AppDomain.CurrentDomain.BaseDirectory, "*.dll");
            foreach (var assemblyFile in assemblies)
            {
                var filename = Path.GetFileName(assemblyFile);
                try
                {
                    var name = AssemblyName.GetAssemblyName(assemblyFile);
                    Assembly.Load(name);
                }
                catch (Exception)
                {
                    throw;
                }
            }
        }

        static void LoadAppAssemblies(List<Assembly> appAssemblies)
        {
            // Avoid loader exceptions by limiting the assemblies
            // those those that can actually contain our services.
            var ourAssemblies = (
                    from asm in AppDomain.CurrentDomain.GetAssemblies()
                    let name = asm.GetName().Name
                    where !name.StartsWith("Microsoft")
                          && !name.StartsWith("System.")
                          && !name.StartsWith("NServiceBus")
                          && !name.StartsWith("xunit", StringComparison.OrdinalIgnoreCase)
                    select asm)
                .ToArray();

            appAssemblies.AddRange(ourAssemblies);
        }

        static void InitializeContainer()
        {
            var afbuilder = new ContainerBuilder();
            var appAssemblies = new List<Assembly>();

            if (appAssemblies.Count == 0)
            {
                PreLoadAllAssemblies();

                // As we did not define which assemblies to load from, we'll load from all.
                LoadAppAssemblies(appAssemblies);
            }

            afbuilder.Register(x => endpointInstance).AsImplementedInterfaces().SingleInstance();

            _autofac = afbuilder.Build();
        }

        static async Task RestartEndpoint()
        {
            try
            {
                RestartAttempts++;

                if (!IsRunning)
                {
                    Console.WriteLine($"Restarting Endpoint attempt: {RestartAttempts}");

                    // Endpoint start-up will throw exception if it does not succeeed in starting up the endpoint.
                    await StartEndpoint().ConfigureAwait(false);

                    Console.WriteLine($"Endpoint restarted successfully after {RestartAttempts} attempt(s).");
                }

                IsRestarting = false;
                RestartAttempts = 0;
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unable to restart the endpoint: {ex.Message}");
            }

            await Task.Delay(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
            await RestartEndpoint().ConfigureAwait(false);
        }

        static async Task OnCriticalError(ICriticalErrorContext endPointContext)
        {
            var fatalMessage = $"The following critical error was encountered: {endPointContext.Error}\nEndpoint will now restart.";
            Console.WriteLine(fatalMessage);

            await endPointContext.Stop().ConfigureAwait(false);

            /* Ednpoint critical error is triggered by several sources:
             *     1. Module queue
             *     2. Module callback queue
             *     3. Every message in-flight recovery failure.
             * only one of these should trigger restart.  */
            var lockTaken = false;
            try
            {
                Monitor.TryEnter(SyncRoot, 2000, ref lockTaken);
                if (lockTaken)
                {
                    IsRunning = false;

                    if (!IsRestarting)
                    {
                        IsRestarting = true;
                        await endpointInstance.Stop().ConfigureAwait(false);
                        await RestartEndpoint().ConfigureAwait(false);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to aquire sync lock and restart module.", ex);
            }
            finally
            {
                if (lockTaken)
                {
                    Monitor.Exit(SyncRoot);
                }
            }
        }

        //protected override void OnStart(string[] args)
        //{
        //    try
        //    {
        //        StartEndpoint().GetAwaiter().GetResult();
        //    }
        //    catch (Exception exception)
        //    {
        //        Console.WriteLine("Failed to start", exception);
        //    }
        //}
        //protected override void OnStop()
        //{
        //    endpointInstance?.Stop().GetAwaiter().GetResult();
        //}
    }
}
