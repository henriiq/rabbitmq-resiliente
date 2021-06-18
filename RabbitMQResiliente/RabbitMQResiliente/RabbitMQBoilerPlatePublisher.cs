using Newtonsoft.Json;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQResiliente
{
    public class RabbitMQBoilerPlatePublisher
    {
        private readonly object publishLock = new object();
        private readonly IConnection _connection;

        private IModel _publisherModel;

        public RabbitMQBoilerPlatePublisher(IConnection connection)
        {
            _connection = connection;
        }

        public bool PublicaMensagem(
            EnderecoPublicacao endereco,
            object entidade,
            IBasicProperties cabecalho = null,
            string retryHeader = "x-retry-count")
        {
            if (endereco == null) return false;

            if (_publisherModel == null || _publisherModel.IsClosed)
            {
                lock (publishLock)
                {
                    _publisherModel = _connection.CreateModel();
                    _publisherModel.ConfirmSelect();
                }
            }

            byte[] body;

            if (entidade is string)
            {
                body = Encoding.UTF8.GetBytes(entidade as string);
            }
            else
            {
                body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(entidade));
            }

            if (cabecalho == null)
                cabecalho = _publisherModel.CreateBasicProperties();

            if (cabecalho != null && !cabecalho.IsHeadersPresent())
                cabecalho.Headers = new Dictionary<string, object>();

            if (cabecalho != null && !cabecalho.Headers.ContainsKey(retryHeader))
                cabecalho.Headers.Add(retryHeader, 0);

            if (cabecalho != null &&
                int.TryParse(
                    cabecalho.Headers[retryHeader].ToString(),
                    out int retryCount))
            {
                retryCount++;
                cabecalho.Headers[retryHeader] = retryCount;
            }

            _publisherModel.BasicPublish(
                exchange: endereco.Exchange, 
                routingKey: endereco.RoutingKey, 
                mandatory: true, 
                basicProperties: cabecalho, 
                body: body);

            return _publisherModel.WaitForConfirms(TimeSpan.FromSeconds(10));
        }
    }
}
