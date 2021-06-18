using RabbitMQ.Client;
using System;
using System.Threading.Tasks;

namespace RabbitMQResiliente
{
    public class RabbitMQBoilerPlate
    {
        private readonly RabbitMQBoilerPlatePublisher _publisher;
        private readonly RabbitMQBoilerPlateConsumer _consumer;
        private readonly RabbitMQBoilerPlateConsumerRetry _consumerRetry;

        public RabbitMQBoilerPlate(IConnection connection)
        {
            _publisher = new RabbitMQBoilerPlatePublisher(connection);
            _consumer = new RabbitMQBoilerPlateConsumer(connection, _publisher);
            _consumerRetry = new RabbitMQBoilerPlateConsumerRetry(connection, _publisher);
        }

        public bool PublicaMensagem(
            EnderecoPublicacao endereco,
            object entidade,
            IBasicProperties cabecalho = null)
        {
            return _publisher.PublicaMensagem(endereco, entidade, cabecalho);
        }

        public RabbitMQBoilerPlate Consume<T>(
            string fila,
            Func<object, Task<MessageResult>> onMessage,
            EnderecoPublicacao enderecoRetry = null)
        {
            _consumer.Consume<T>(fila, onMessage, enderecoRetry);
            return this;
        }

        public RabbitMQBoilerPlate Consume<T>(
            string fila,
            Func<object, MessageResult> onMessage,
            EnderecoPublicacao enderecoRetry = null)
        {
            Consume<T>(fila, (m) => Task.FromResult(onMessage(m)), enderecoRetry);
            return this;
        }

        public RabbitMQBoilerPlate Retry<T>(
            string fila,
            Func<object, Task<MessageResult>> onMessage,
            int maxRetry = 5,
            string retryHeader = "x-retry-count")
        {
            _consumerRetry.Retry<T>(fila, onMessage, maxRetry, retryHeader);
            return this;
        }

        public RabbitMQBoilerPlate Retry<T>(
            string fila,
            Func<object, MessageResult> onMessage,
            int maxRetry = 5,
            string retryHeader = "x-retry-count")
        {
            Retry<T>(fila, (m) => Task.FromResult(onMessage(m)), maxRetry, retryHeader);
            return this;
        }
    }
}
