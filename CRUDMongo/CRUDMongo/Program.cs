using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace CRUDMongo
{
    class Program
    {
        static void Main(string[] args)
        {
            CountryMovies();
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
    }
}