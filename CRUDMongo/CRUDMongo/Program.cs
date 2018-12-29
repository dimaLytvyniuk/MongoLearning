using System;
using System.Collections.Generic;
using System.Linq;
using DnsClient.Protocol;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace CRUDMongo
{
    class Program
    {
        static void Main(string[] args)
        {
            TestEnumString();
        }

        static void Movies()
        {
            var client = new MongoClient();
            var database = client.GetDatabase("video");
            var collection = database.GetCollection<BsonDocument>("movieDetails");

            var filter = Builders<BsonDocument>.Filter.Eq("rated", "PG-13") &
                Builders<BsonDocument>.Filter.Eq("awards.wins", 0);
                
            var documents = collection.Find(filter)
                .Project(Builders<BsonDocument>.Projection.Include("title"))
                .ToList();

            foreach (var document in documents)
            {
                Console.WriteLine(document);    
            }            
        }

        static void CountryMovies()
        {
            var client = new MongoClient();
            var database = client.GetDatabase("video");
            var collection = database.GetCollection<BsonDocument>("movieDetails");

            var filter = Builders<BsonDocument>.Filter.Eq("countries.1", "Sweden");
            
            var documents = collection.Find(filter)
                .Project(Builders<BsonDocument>.Projection.Include("countries"))
                .ToList();

            foreach (var document in documents)
            {
                Console.WriteLine(document);    
            }
        }
        
        static void Crud()
        {
            var client = new MongoClient();
            var database = client.GetDatabase("students");
            var collection = database.GetCollection<BsonDocument>("grades");

            var filter = Builders<BsonDocument>.Filter.Eq("type", "homework");
            var documents = collection.Find(filter).ToList();

            var grouping = documents.GroupBy(x => x.GetValue("student_id"), x => x.GetValue("score")).ToDictionary(x => x.Key, x => x.Min());
            
            var deleteModels = new List<WriteModel<BsonDocument>>();
            
            foreach (var document in grouping)
            {
                var deleteDocument = Builders<BsonDocument>.Filter.Eq("student_id", document.Key)
                                     & Builders<BsonDocument>.Filter.Eq("score", document.Value);
                deleteModels.Add(new DeleteOneModel<BsonDocument>(deleteDocument));
            }

            collection.BulkWrite(deleteModels);
        }

        static void SchemaTask()
        {
            var client = new MongoClient();
            var database = client.GetDatabase("school");
            var collection = database.GetCollection<BsonDocument>("students");

            var documents = collection.Find(new BsonDocument()).ToList();

            var updateModels = new List<WriteModel<BsonDocument>>();
            
            foreach (var document in documents)
            {
                var minValue = document
                    .GetValue("scores")
                    .AsBsonArray
                    .Values
                    .AsEnumerable()
                    .Select(x => x.AsBsonDocument)
                    .Where(x => x.GetValue("type") == "homework")
                    .Min(x => x.GetValue("score").AsDouble);

                var filter = Builders<BsonDocument>
                    .Filter
                    .Eq("_id", document.GetValue("_id"));
                
                var deleteValue = new BsonDocument().Add("score", minValue);
                var pullModel = Builders<BsonDocument>
                    .Update
                    .Pull("scores", deleteValue);
                
                updateModels.Add(new UpdateOneModel<BsonDocument>(filter, pullModel));
            }

            collection.BulkWrite(updateModels);
        }
        
        static void TestEnumString()
        {
            var client = new MongoClient();
            var database = client.GetDatabase("myEnums");
            var collection = database.GetCollection<Sock>("socks");
            
            collection.InsertOne(new Sock { Type = Type.ABC});

            var list = collection.Find(x => true).ToList();

            foreach (var sock in list)
            {
                Console.WriteLine($"{sock.Id} {sock.Type}");
            }
        }
    }
    
    class Sock
    {
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }
        
        [BsonRepresentation(BsonType.String)]
        public Type Type { get; set; }
    }

    enum Type
    {
        ABC,
        DBA
    }
}