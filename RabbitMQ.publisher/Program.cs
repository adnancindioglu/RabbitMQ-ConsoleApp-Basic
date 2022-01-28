using RabbitMQ.Client;
using System;
using System.Linq;
using System.Text;

namespace RabbitMQ.publisher
{
    class Program
    {
        static void Main(string[] args)
        {
            NotExchange();
            //FanoutExchange();
            //DirectExchange();
        }

        private static void NotExchange()
        {
            var factory = new ConnectionFactory();

            factory.Uri = new Uri("amqps://pzzrnikj:ljZ9pbzuot5AdM2C4NSj3tdrTS1HcoLy@baboon.rmq.cloudamqp.com/pzzrnikj");

            //bağlantı oluşturuyoruz
            using var connection = factory.CreateConnection();

            //kanal oluşturuyoruz
            var channel = connection.CreateModel();

            //queue:kanal adı
            //durable:false->kuyruktaki verilen memory tutulur(uygulama durursa kaybolur)  true->kuyruktaki veriler fiziksel kayıt edilir(veriler kaybolmaz)
            //exclusive:false-> kuyruğa farklı bir kanalla ulaşabilirsin true:kuyruğa sadece channel kanalıyla bağlanabilirsin
            //autoDelete:true-> subscriber kapanırsa yada bağlantı koparsa kuyruğu siler
            channel.QueueDeclare("hello-queue", true, false, false);

            //kuyruğa 50 mesaj gönderelim
            Enumerable.Range(1, 50).ToList().ForEach(x =>
            {
                string message = $"Message {x}";

                byte[] messageBody = Encoding.UTF8.GetBytes(message);

                //channel kanalıla gönderiyoruz
                //exchange yoksa default exchange kullanılır ve routingKey kanal adı verilir
                channel.BasicPublish(string.Empty, "hello-queue", null, messageBody);

                Console.WriteLine($"Mesajınız Gönderilmiştir: {message}");
            });





            Console.ReadLine();
        }

        private static void FanoutExchange()
        {
            //Aldığı mesajı tüm kuyruklara gönderir
            //Queues'nin Subscriber tarafında oluşturulmasının sebebi Subscriber kapandımı mesajın kuyrukta kalmaması amaçlanır(seneryoya göre değişir)

            var factory = new ConnectionFactory();

            factory.Uri = new Uri("amqps://pzzrnikj:ljZ9pbzuot5AdM2C4NSj3tdrTS1HcoLy@baboon.rmq.cloudamqp.com/pzzrnikj");

            using var connection = factory.CreateConnection();

            var channel = connection.CreateModel();

            channel.ExchangeDeclare("logs-fanout", type: ExchangeType.Fanout, durable: true);

            Enumerable.Range(1, 50).ToList().ForEach(x =>
            {
                string message = $"log {x}";

                byte[] messageBody = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish("logs-fanout", "", null, messageBody);

                Console.WriteLine($"Mesajınız Gönderilmiştir: {message}");
            });

            Console.ReadLine();
        }

        private static void DirectExchange()
        {
            //publisher gelen mesajın route göre ilgili kuyruğa gönderir
            //kuyruğu publisher tarafında oluşturacağız(seneryoya göre değişir)

            var factory = new ConnectionFactory();

            factory.Uri = new Uri("amqps://pzzrnikj:ljZ9pbzuot5AdM2C4NSj3tdrTS1HcoLy@baboon.rmq.cloudamqp.com/pzzrnikj");

            using var connection = factory.CreateConnection();

            var channel = connection.CreateModel();

            channel.ExchangeDeclare("logs-direct", type: ExchangeType.Direct, durable: true);

            Enum.GetNames(typeof(LogNames)).ToList().ForEach(x =>
            {
                var routeKey = $"route-{x}";

                var queueName = $"direct-queue-{x}";

                channel.QueueDeclare(queueName, true, false, false);

                channel.QueueBind(queueName, "logs-direct", routeKey, null);

            });

            Enumerable.Range(1, 50).ToList().ForEach(x =>
            {
                LogNames log = (LogNames)new Random().Next(1, 5);

                string message = $"log-type: {log}";

                var routeKey = $"route-{log}";

                byte[] messageBody = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish("logs-direct", routeKey, null, messageBody);

                Console.WriteLine($"Log Gönderilmiştir: {message}");
            });

            Console.ReadLine();
        }

        public enum LogNames
        {
            Critical = 1,
            Error = 2,
            Warning = 3,
            Info = 4
        }
    }
}
