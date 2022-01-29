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
            //NotExchange();
            //FanoutExchange();
            //DirectExchange();
            //TopicExchange();
            HeaderExchange();
        }

        private static void NotExchange()
        {
            var factory = new ConnectionFactory();

            factory.Uri = new Uri("");

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

            factory.Uri = new Uri("");

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

            factory.Uri = new Uri("");

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

        private static void TopicExchange()
        {
            //direct benzer fakat routeKey gönderim şekli, farklı
            //routeKey ler birden fazla olabilir
            //routeKey kullanımı "Critical.Warning.Error" şeklinde üç key tek key ile tanımlanabilinir
            //alıcı tarafında "Critical.Warning.Error" veya "*.Warning.*" şeklinde olabilir * herhangibir değer anlamına gelir
            // * dışında # ilede kullanılı "#.Error" sonu error olanları getir
            //çok fazla varyasyon olduğu için kuyruklar subscriber tarafında oluşturulması daha iyi olabilir

            var factory = new ConnectionFactory();

            factory.Uri = new Uri("");

            using var connection = factory.CreateConnection();

            var channel = connection.CreateModel();

            channel.ExchangeDeclare("logs-topic", type: ExchangeType.Topic, durable: true);

            Random rdm = new Random();

            Enumerable.Range(1, 50).ToList().ForEach(x =>
            {
                LogNames log1 = (LogNames)rdm.Next(1, 5);
                LogNames log2 = (LogNames)rdm.Next(1, 5);
                LogNames log3 = (LogNames)rdm.Next(1, 5);

                var routeKey = $"{log1}.{log2}.{log3}";

                string message = $"log-type: {log1}-{log2}-{log3}";

                byte[] messageBody = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish("logs-topic", routeKey, null, messageBody);

                Console.WriteLine($"Log Gönderilmiştir: {message}");
            });

            Console.ReadLine();
        }

        private static void HeaderExchange()
        {
            //topicte routerkey lere göre gönderim yapılıyordu
            //header ise header bilgisine göre gönderim yapılıyor
            //header bilgileri key value şeklinde gönderiliyor
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
