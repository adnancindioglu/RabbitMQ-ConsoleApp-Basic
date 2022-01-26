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
            var factory = new ConnectionFactory();

            factory.Uri = new Uri("amqps://pzzrnikj:ljZ9pbzuot5AdM2C4NSj3tdrTS1HcoLy@baboon.rmq.cloudamqp.com/pzzrnikj");

            using var connection = factory.CreateConnection();

            var channel = connection.CreateModel();

            //kanal publisher tarafında oluşturulduğu için burda oluşturmaya gerek kalmaz
            //channel.QueueDeclare("hello-queue", true, false, false);

            //prefetchSize:alınacak mesajın boyutu 0-> herhangibir boyuttta gönderebilirsin
            //prefetchCount:her bir subscriber kaç mesaj alsın
            //global:true->prefetchCount sayıyı subscriber eşit şekilde paylaştırır false->her subscriber prefetchCount sayısında gönderim yapar
            channel.BasicQos(0,1,false);

            var consumer = new EventingBasicConsumer(channel);

            //autoAck:true-> message doğruda işlense yanlışta işlense kuyruktan siler false->mesajı işledikten sonra silmen için haberdar edeceğim
            channel.BasicConsume("hello-queue",false,consumer);

            consumer.Received += (object sender, BasicDeliverEventArgs e)=> 
            {
                var message = Encoding.UTF8.GetString(e.Body.ToArray());

                Thread.Sleep(1500);
                Console.WriteLine(message);

                //ilgili mesaj işlenmişse kuyruktan sil
                channel.BasicAck(e.DeliveryTag, false);
            };
          

            Console.ReadLine();
        }

        
    }
}
