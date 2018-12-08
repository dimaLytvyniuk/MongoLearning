using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace Exam
{
    class Program
    {
        static void Main(string[] args)
        {
            Seven();
        }

        static void First()
        {
            var client = new MongoClient();
            var database = client.GetDatabase("enron");
            var collection = database.GetCollection<BsonDocument>("messages");

            var filter = Builders<BsonDocument>.Filter.Eq("headers.From", "andrew.fastow@enron.com") &
                Builders<BsonDocument>.Filter.Eq("headers.To",  "jeff.skilling@enron.com");
            
            var count = collection.Find(filter).CountDocuments();
            
            Console.WriteLine(count);
        }

        static void Second()
        {
            var client = new MongoClient();
            var database = client.GetDatabase("enron");
            var collection = database.GetCollection<Message>("messages");

            var aggregationCollection = collection.Aggregate();

            aggregationCollection.Options.AllowDiskUse = true;
            
            var result = aggregationCollection
                .Unwind(x => x.Header.To)
                .Group(new BsonDocument
                {
                    {"_id", new BsonDocument {{"_id", "$_id"}, { "from", "$headers.From"}, { "to", "$headers.To"}}},
                })
                .Group(new BsonDocument
                {
                    {"_id", new BsonDocument {{ "from", "$_id.from"}, { "to", "$_id.to"}}},
                    { "count", new BsonDocument("$sum", 1)}
                })
                .Sort(new BsonDocument("count", -1))
                .Limit(10)
                .ToList();

            foreach (var value in result)
            {
                Console.WriteLine(value);   
            }
        }

        static void Three()
        {
            var client = new MongoClient();
            var database = client.GetDatabase("enron");
            var collection = database.GetCollection<Message>("messages");

            var result = collection.UpdateOne(
                new BsonDocument("headers.Message-ID", "<8147308.1075851042335.JavaMail.evans@thyme>"),
                new BsonDocument("$push",
                    new BsonDocument("headers.To", "mrpotatohead@mongodb.com")));
            
            Console.WriteLine(result);

            var entity = collection.Find(x => x.Header.MessageId == "<8147308.1075851042335.JavaMail.evans@thyme>").ToList();

            foreach (var to in entity[0].Header.To)
            {
                Console.WriteLine(to);
            }
        }
        
        static void Seven()
        {
            var client = new MongoClient();
            var database = client.GetDatabase("photos");
            var albums = database.GetCollection<Album>("albums");
            var images = database.GetCollection<Image>("images");

            var indexAlbums = Builders<Album>.IndexKeys
                .Ascending(x => x.Images);

            albums.Indexes.CreateOne(indexAlbums);
            
            var imagesToDrop = new List<long>();
            
            using (var cursor = images.FindSync(new BsonDocument()))
            {
                while (cursor.MoveNext())
                {
                    var imagesChunck = cursor.Current;
                    foreach (var doc in imagesChunck)
                    {
                        var album = albums.Find(x => x.Images.Contains(doc.Id)).Limit(1);
                        if (album.CountDocuments() == 0)
                        {
                            imagesToDrop.Add(doc.Id);
                        }
                    }
                }
            }

            var delete = Builders<Image>.Filter
                .In(x => x.Id, imagesToDrop);

            var result = images.DeleteMany(delete);
            
            Console.WriteLine(result);
        }
        
        class Message
        {
            public ObjectId Id { get; set; }
            [BsonElement("body")]
            public string Body { get; set; }
            [BsonElement("filename")]
            public string Filename { get; set; }
            [BsonElement("headers")]
            public Header Header { get; set; } 
            [BsonElement("mailbox")]
            public string Mailbox { get; set; }
            [BsonElement("subFolder")]
            public string SubFolder { get; set; }
        }

        class Header
        {
            [BsonElement("Content-Transfer-Encoding")]
            public string ContentTransferEncoding { get; set; }
            [BsonElement("Content-Type")]
            public string ContentType { get; set; }
            public DateTime Date { get; set; }
            public string From { get; set; }
            [BsonElement("Message-ID")]
            public string MessageId { get; set; }
            [BsonElement("Mime-Version")]
            public string MimeVersion { get; set; }
            public string Subject { get; set; }
            public List<string> To { get; set; }
            [BsonElement("X-FileName")]
            public string XFileName { get; set; }
            [BsonElement("X-Folder")]
            public string XFolder { get; set; }
            [BsonElement("X-From")]
            public string XFrom { get; set; }
            [BsonElement("X-Origin")]
            public string XOrigin { get; set; }
            [BsonElement("X-To")]
            public string XTo { get; set; }
            [BsonElement("X-bcc")]
            public string XBcc { get; set; }
            [BsonElement("X-cc")]
            public string Xcc { get; set; }
        }

        class Album
        {
            public long Id { get; set; }
            [BsonElement("images")]
            public List<long> Images { get; set; }
        }

        class Image
        {
            public long Id { get; set; }
            [BsonElement("height")]
            public int Height { get; set; }
            [BsonElement("width")]
            public int Width { get; set; }
            [BsonElement("tags")]
            public List<string> Tags { get; set; }
        }
    }
}