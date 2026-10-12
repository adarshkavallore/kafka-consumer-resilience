using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace DynamicConsumer
{
    public sealed class RedisProcessedStateStore
    {
        private readonly IDatabase _db;
        private readonly string _key;

        public RedisProcessedStateStore(IConnectionMultiplexer mux, string groupId, string namespaceId)
        {
            _db = mux.GetDatabase();
            _key = $"dyn:{groupId}:{namespaceId}:offset";
        }

        /// <summary>Returns the last durably-processed offset for a partition, or null if none recorded yet.</summary>
        public long? Read(int partition)
        {
            var value = _db.StringGet(_key);
            if (!value.HasValue) return null;
            return long.TryParse(value, out var offset) ? offset : null;
        }

        /// <summary>Durably records lastProcessedOffset=offset for a partition.</summary>
        public void Write(int partition, long offset)
        {
            _db.StringSet(_key, offset);
        }
    }
}
