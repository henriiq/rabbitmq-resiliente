using RabbitMQ.Client;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace RabbitMQResiliente
{
    class Program
    {
        static void Main(string[] args)
        {
            var conn = new ConnectionFactory().CreateConnection();
            var chnn = conn.CreateModel();

            var q = "q.res";
            var x = "x.res";
            var r = "r.res";

            var retry_q = "q.res_retry";
            var retry_x = "x.res_retry";
            var retry_r = "r.res_retry";

            chnn.ExchangeDeclare(x, ExchangeType.Direct, true);
            chnn.QueueDeclare(q, true, false, false);
            chnn.QueueBind(q, x, r, null);

            chnn.ExchangeDeclare(retry_x, ExchangeType.Direct, true);
            chnn.QueueDeclare(retry_q, true, false, false);
            chnn.QueueBind(retry_q, retry_x, retry_r, null);

            var rab = new RabbitMQBoilerPlate(conn);

            Enumerable.Range(0, 100).ToList().ForEach(f =>
                rab
                    .PublicaMensagem(new EnderecoPublicacao(x, r), "Henrique Gustavo"));

            rab
                .Consume<string>(q, onMessage: async (m) =>
                {
                    Console.WriteLine($"A - {m}");

                    await Task.Delay(3000);
                    return MessageResult.Retry;
                }, 
                enderecoRetry: new EnderecoPublicacao(retry_x, retry_r))

                .Consume<string>(q, onMessage: async (m) =>
                {
                    Console.WriteLine($"B - {m}");

                    await Task.Delay(1000);
                    return MessageResult.Retry;
                })

                .Retry<string>(retry_q, onMessage: async (m) =>
                {
                    Console.WriteLine($"Retry - {m}");

                    await Task.Delay(300);
                    return MessageResult.Retry;
                }, maxRetry: 100);


            Console.ReadKey();
        }
    }
}
