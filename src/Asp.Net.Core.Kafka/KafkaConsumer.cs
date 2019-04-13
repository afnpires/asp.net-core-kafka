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

    public class KafkaConsumer
    {
        private readonly IDictionary<string, object> configurations;

        public KafkaConsumer()
        {
            this.configurations = new Dictionary<string, object>
            {
                { "bootstrap.servers", "localhost:9092" },
                { "group.id", "example-1" }
            };
        }

        public void MyConsume()
        {
            Task.Run(() =>
            {
                Debug.WriteLine("Starting consumer");
                using (var consumer = new Consumer<string, string>(this.configurations, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
                {
                    consumer.OnError += this.ConsumerOnError;

                    consumer.Subscribe("my.awesome.topic");

                    while (true)
                    {
                        var consumedSuccessfully = consumer.Consume(out var message, 5000);
                        if (message is object)
                        {
                            Debug.WriteLine($"Consumed {message.Key}:{message.Value}");
                        }
                    }
                }
            });
        }

        private void ConsumerOnError(object sender, Error e)
        {
            Debug.WriteLine($"Error {e.Reason}");
        }
    }
}
