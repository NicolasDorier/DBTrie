using DBTrie.Storage;
using DBTrie.TrieModel;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie
{
	public class DBTrieEngine : IAsyncDisposable
	{
		List<CacheStorage> caches = new List<CacheStorage>();
		public Schema Schema { get; }
		public static async ValueTask<DBTrieEngine> OpenFromFolder(string folderName)
		{
			if (folderName == null)
				throw new ArgumentNullException(nameof(folderName));
			var schemaPath = Path.Combine(folderName, "_DBreezeSchema");
			var fs = new FileStorage(schemaPath);
			var cache = new CacheStorage(fs, true);
			var trie = await LTrie.OpenFromStorage(fs);
			var schema = await Schema.OpenFromTrie(trie);
			return new DBTrieEngine(schema, cache);
		}

		internal DBTrieEngine(Schema schema, CacheStorage schemaCache)
		{
			if (schema == null)
				throw new ArgumentNullException(nameof(schema));
			Schema = schema;
			caches.Add(schemaCache);
		}

		public ValueTask DisposeAsync()
		{
			throw new NotImplementedException();
		}
	}
}
