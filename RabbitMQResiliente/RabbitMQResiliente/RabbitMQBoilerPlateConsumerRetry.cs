using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQResiliente
{
    public class RabbitMQBoilerPlateConsumerRetry
    {
        private readonly IConnection _connection;
        private readonly RabbitMQBoilerPlatePublisher _publisher;

        public RabbitMQBoilerPlateConsumerRetry(IConnection connection, RabbitMQBoilerPlatePublisher publisher)
        {
            _connection = connection;
            _publisher = publisher;
        }

        public void Retry<T>(
                string fila,
                Func<object, Task<MessageResult>> onMessage,
                int maxRetry,
                string retryHeader)
        {
            var model = _connection.CreateModel();
            var consumer = new EventingBasicConsumer(model); 

            consumer.Received += async (s, e) =>
            {
                IBasicProperties basicProp;
                MessageResult r;
                object entidade;

                try
                {
                    var deliveryTag = e.DeliveryTag;

                    if (e.BasicProperties == null)
                        e.BasicProperties = model.CreateBasicProperties();

                    if (e.BasicProperties != null && !e.BasicProperties.IsHeadersPresent())
                        e.BasicProperties.Headers = new Dictionary<string, object>();

                    basicProp = e.BasicProperties;

                    if (typeof(T) == typeof(string))
                    {
                        entidade = Encoding.UTF8.GetString(e.Body.ToArray());
                        r = await onMessage(entidade);
                    }
                    else
                    {
                        entidade =
                            JsonConvert.DeserializeObject<T>(
                                Encoding.UTF8.GetString(e.Body.ToArray()));
                        r = await onMessage(entidade);
                    }

                    if (basicProp.Headers.ContainsKey(retryHeader)
                    && int.TryParse(basicProp.Headers[retryHeader].ToString(), out int retryCount)
                    && retryCount >= maxRetry)
                    {
                        r = MessageResult.Reject;
                    }

                    switch (r)
                    {
                        case MessageResult.Ok:
                            model.BasicAck(deliveryTag, false);
                            break;
                        case MessageResult.Retry:
                            _publisher.PublicaMensagem(new EnderecoPublicacao(e.Exchange, e.RoutingKey), entidade, basicProp);
                            model.BasicAck(deliveryTag, false);
                            break;
                        case MessageResult.Reject:
                            model.BasicReject(deliveryTag, false);
                            break;
                    }

                }
                catch (Exception x)
                {
                    Console.WriteLine(x);
                }
            };

            model.BasicQos(0, 10, false);
            model.BasicConsume(fila, false, consumer);
        }
    }
}
