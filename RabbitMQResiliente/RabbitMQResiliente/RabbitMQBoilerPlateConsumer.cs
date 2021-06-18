using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQResiliente
{
    public class RabbitMQBoilerPlateConsumer
    {
        private readonly IConnection _connection;
        private readonly RabbitMQBoilerPlatePublisher _publisher;

        public RabbitMQBoilerPlateConsumer(IConnection connection, RabbitMQBoilerPlatePublisher publisher)
        {
            _connection = connection;
            _publisher = publisher;
        }

        public void Consume<T>(
            string fila,
            Func<object, Task<MessageResult>> onMessage,
            EnderecoPublicacao enderecoRetry = null)
        {
            var model = _connection.CreateModel();
            var consumer = new EventingBasicConsumer(model);

            consumer.Received += async (s, e) =>
            {
                try
                {
                    var deliveryTag = e.DeliveryTag;

                    MessageResult r;
                    IBasicProperties basicProp;
                    object entidade;

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

                    switch (r)
                    {
                        case MessageResult.Ok:
                            model.BasicAck(deliveryTag, false);
                            break;
                        case MessageResult.Retry:
                            model.BasicAck(deliveryTag, false);
                            _publisher.PublicaMensagem(enderecoRetry, entidade, basicProp);
                            break;
                        case MessageResult.Reject:
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
