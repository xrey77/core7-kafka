using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Confluent.Kafka;
using System;
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;
using System.Diagnostics;
using core7_kafka.Models;

namespace core7_kafka.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ProducerController : ControllerBase {

        private readonly string
        bootstrapServers = "localhost:9092";
        private readonly string topic = "test";

        [HttpPost("/producer")]
        public async Task<IActionResult> CreateMessage([FromBody] UserModel usermodel) {

            string message = JsonSerializer.Serialize(usermodel);
            await SendOrderRequest(topic, message);
            return Ok(new {statuscode = 200, message="Sent successfully."});
            
        }        


        private async Task < bool > SendOrderRequest(string topic, string message) {

                ProducerConfig config = new ProducerConfig {
                    BootstrapServers = bootstrapServers,
                    ClientId = Dns.GetHostName()
                };

                try {
                    using(var producer = new ProducerBuilder<Null, string> (config).Build()) {
                        var result = await producer.ProduceAsync(topic, new Message <Null, string> {
                            Value = message
                    });

                    return await Task.FromResult(true);
                    // return Ok(new {statuscode = 200, message ="Ok"}); 
                   }
                } catch(Exception ex) {
                    Console.WriteLine(ex.Message);
                    // return Ok(new {statuscode = 500, message = ex.Message});
                }
            

                return await Task.FromResult(false);

        }
    }    
}