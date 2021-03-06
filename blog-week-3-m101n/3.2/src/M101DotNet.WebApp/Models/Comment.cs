﻿using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace M101DotNet.WebApp.Models
{
    public class Comment
    {
        // XXX WORK HERE
        // Add in the appropriate properties.
        // The homework instructions have the
        // necessary schema.
        public string Author { get; set; }
        public string Content { get; set; }
        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime CreatedAtUtc { get; set; }
    }
}