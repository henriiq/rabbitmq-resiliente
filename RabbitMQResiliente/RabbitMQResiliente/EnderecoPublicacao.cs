namespace RabbitMQResiliente
{
    public class EnderecoPublicacao
    {
        public EnderecoPublicacao(string exchange, string routingKey)
        {
            Exchange = exchange;
            RoutingKey = routingKey;
        }

        public string Exchange { get; set; }
        public string RoutingKey { get; set; }
    }
}
