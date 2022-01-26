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
            Enumerable.Range(1, 50).ToList().ForEach (x => 
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
    }
}
