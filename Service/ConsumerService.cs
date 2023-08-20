using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using core7_kafka.Models;

namespace core7_kafka.Service
{

    public class ConsumerService:IHostedService {

        private readonly string topic = "test";
        private readonly string groupId = "test_group";
        private readonly string bootstrapServers = "localhost:9092";

        public Task StartAsync(CancellationToken cancellationToken) {
            var config = new ConsumerConfig {
            GroupId = groupId,
            BootstrapServers = bootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest
            };

            try {
                using(var consumerBuilder = new ConsumerBuilder 
                <Ignore, string> (config).Build()) {
                    consumerBuilder.Subscribe(topic);
                    var cancelToken = new CancellationTokenSource();

                try {
                    while (true) {
                        var consumer = consumerBuilder.Consume 
                           (cancelToken.Token);
                        var userRequest = JsonSerializer.Deserialize <UserModel>(consumer.Message.Value);
                        Console.WriteLine("Processing User Id: {} " + userRequest.UserId);
                    }
                } catch (OperationCanceledException) {
                    consumerBuilder.Close();
                }
            }
        } catch (Exception ex) {
            Console.WriteLine(ex.Message);
        }

        return Task.CompletedTask;
        }
        public Task StopAsync(CancellationToken cancellationToken) {
            return Task.CompletedTask;
        }
    }
    
}