namespace Asp.Net.Core.Kafka
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using Confluent.Kafka.Serialization;
    using Microsoft.Extensions.Hosting;

    public class KafkaHostedConsumer : IHostedService, IDisposable
    {
        private readonly IDictionary<string, object> configurations;
        private readonly Consumer<string, string> consumer;

        public KafkaHostedConsumer()
        {
            this.configurations = new Dictionary<string, object>
            {
                { "bootstrap.servers", "localhost:9092" },
                { "group.id", "local-consumer" }
            };
            this.consumer = new Consumer<string, string>(this.configurations, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8));
            Debug.WriteLine("Starting consumer");
        }

        public void Dispose()
        {
            Debug.WriteLine("Calling dispose");
            this.consumer?.Dispose();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            this.consumer.OnError += this.ConsumerOnError;

            this.consumer.Subscribe(new List<string>
            {
                "my.awesome.topic",
                "ninguem.para.o.benfica"
            });

            while (true)
            {
                var consumedSuccessfully = this.consumer.Consume(out var message, 5000);
                if (message is object)
                {
                    Debug.WriteLine($"Consumed {message.Key}:{message.Value}");
                    await this.consumer.CommitAsync().ConfigureAwait(false);
                    Debug.WriteLine("Committed offsets");
                }
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Debug.WriteLine("Disposing consumer");
            this.consumer.Dispose();
            Debug.WriteLine("Consumer disposed");

            return Task.CompletedTask;
        }

        private void ConsumerOnError(object sender, Error e)
        {
            Debug.WriteLine($"Error {e.Reason}");
        }
    }
}
