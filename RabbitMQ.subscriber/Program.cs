using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace RabbitMQ.subscriber
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

            factory.Uri = new Uri("");

            using var connection = factory.CreateConnection();

            var channel = connection.CreateModel();

            //kanal publisher tarafında oluşturulduğu için burda oluşturmaya gerek kalmaz
            //channel.QueueDeclare("hello-queue", true, false, false);

            //prefetchSize:alınacak mesajın boyutu 0-> herhangibir boyuttta gönderebilirsin
            //prefetchCount:her bir subscriber kaç mesaj alsın
            //global:true->prefetchCount sayıyı subscriber eşit şekilde paylaştırır false->her subscriber prefetchCount sayısında gönderim yapar
            channel.BasicQos(0, 1, false);

            var consumer = new EventingBasicConsumer(channel);

            //autoAck:true-> message doğruda işlense yanlışta işlense kuyruktan siler false->mesajı işledikten sonra silmen için haberdar edeceğim
            channel.BasicConsume("hello-queue", false, consumer);

            consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            {
                var message = Encoding.UTF8.GetString(e.Body.ToArray());

                Thread.Sleep(1500);
                Console.WriteLine(message);

                //ilgili mesaj işlenmişse kuyruktan sil
                channel.BasicAck(e.DeliveryTag, false);
            };


            Console.ReadLine();
        }

        private static void FanoutExchange()
        {
            var factory = new ConnectionFactory();

            factory.Uri = new Uri("");

            using var connection = factory.CreateConnection();

            var channel = connection.CreateModel();

            channel.BasicQos(0, 1, false);

            //kuyruğu subscriber tarafında oluştururuz
            //kuyruk adı random olmalı çünkü farklı subscriberlar aynı kuyruğu dinlememeli
            var randomQueueName = channel.QueueDeclare().QueueName;

            //kalıcı kuyruk için
            //kuyruğu kendimizde oluşturabiliriz randomQueueName belli bir isim vermemiz gerekir
            //bu sayede kuyruktaki mesajlar kaybolmaz tekrar çalışınca kaldığı yerden devam eder ve yeni mesajlar kapalı olsa daki birikir
            //***channel.QueueDeclare(randomQueueName, true, false, false);

            //uygulama kapandığında kuyruğu silmemize yarar
            channel.QueueBind(randomQueueName, "logs-fanout", "", null);

            var consumer = new EventingBasicConsumer(channel);

            channel.BasicConsume(randomQueueName, false, consumer);

            Console.WriteLine("Logları dinliyorum...");

            consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            {
                var message = Encoding.UTF8.GetString(e.Body.ToArray());

                Thread.Sleep(1500);
                Console.WriteLine(message);

                channel.BasicAck(e.DeliveryTag, false);
            };


            Console.ReadLine();
        }

        private static void DirectExchange()
        {
            var factory = new ConnectionFactory();

            factory.Uri = new Uri("");

            using var connection = factory.CreateConnection();

            var channel = connection.CreateModel();

            channel.BasicQos(0, 1, false);

            var consumer = new EventingBasicConsumer(channel);

            //farklı kuyruklardaki mesajları alabiliriz
            var queueName = "direct-queue-Critical";
            //var queueName = "direct-queue-Error";
            //var queueName = "direct-queue-Warning";
            //var queueName = "direct-queue-Info";

            channel.BasicConsume(queueName, false, consumer);

            Console.WriteLine("Logları dinliyorum...");

            consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            {
                var message = Encoding.UTF8.GetString(e.Body.ToArray());

                Thread.Sleep(1500);
                Console.WriteLine(message);

                channel.BasicAck(e.DeliveryTag, false);
            };


            Console.ReadLine();
        }


    }
}
