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
            Action<Exception> onError = null,
            EnderecoPublicacao enderecoRetry = null,
            ushort qtdMensagensSimultaneas = 1)
        {
            onError ??= x => Console.WriteLine(x);
            _consumer.Consume<T>(
                fila,
                onMessage,
                onError,
                enderecoRetry,
                qtdMensagensSimultaneas);

            return this;
        }

        public RabbitMQBoilerPlate Consume<T>(
            string fila,
            Func<object, MessageResult> onMessage,
            Action<Exception> onError = null,
            EnderecoPublicacao enderecoRetry = null,
            ushort qtdMensagensSimultaneas = 1)
        {
            onError ??= x => Console.WriteLine(x);
            Consume<T>(
                fila,
                (m) => Task.FromResult(onMessage(m)),
                onError,
                enderecoRetry,
                qtdMensagensSimultaneas);

            return this;
        }

        public RabbitMQBoilerPlate Retry<T>(
            string fila,
            Func<object, Task<MessageResult>> onMessage,
            Action<Exception> onError = null,
            int maxRetry = 5,
            string retryHeader = "x-retry-count",
            ushort qtdMensagensSimultaneas = 1)
        {
            onError ??= x => Console.WriteLine(x);
            _consumerRetry.Retry<T>(
                fila,
                onMessage,
                onError,
                maxRetry,
                retryHeader,
                qtdMensagensSimultaneas);

            return this;
        }

        public RabbitMQBoilerPlate Retry<T>(
            string fila,
            Func<object, MessageResult> onMessage,
            Action<Exception> onError = null,
            int maxRetry = 5,
            string retryHeader = "x-retry-count",
            ushort qtdMensagensSimultaneas = 1)
        {
            onError ??= x => Console.WriteLine(x);
            Retry<T>(
                fila,
                (m) => Task.FromResult(onMessage(m)),
                onError,
                maxRetry,
                retryHeader,
                qtdMensagensSimultaneas);

            return this;
        }
    }
}
