using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
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
            Action<Exception> onError,
            EnderecoPublicacao enderecoRetry,
            ushort qtdMensagensSimultaneas)
        {
            var model = _connection.CreateModel();
            var consumer = new EventingBasicConsumer(model);

            consumer.Received += async (s, e) =>
            {
                MessageResult r;
                object entidade;

                try
                {
                    var deliveryTag = e.DeliveryTag;
                    var basicProp = e.BasicProperties;

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

                    if (r == MessageResult.Retry && enderecoRetry == null)
                    {
                        onError(
                            new Exception(
                                $"O processamento da mensagem retornou retentativa mas o endereco de retentativa nao foi configurado." +
                                $"MSG: {entidade}"));

                        r = MessageResult.Reject;
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
                            model.BasicReject(deliveryTag, false);
                            break;
                    }
                }
                catch (Exception x)
                {
                    onError(x);
                }
            };

            model.BasicQos(0, qtdMensagensSimultaneas, false);
            model.BasicConsume(fila, false, consumer);
        }
    }
}
