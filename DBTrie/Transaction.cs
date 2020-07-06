using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie
{
	public class Transaction
	{
		Dictionary<string, LTrie> _Tables = new Dictionary<string, LTrie>();
		public async ValueTask Insert(string tableName, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
		{
			if (tableName == null)
				throw new ArgumentNullException(nameof(tableName));
			var ltrie = GetTable(tableName);
		}

		private async ValueTask<LTrie> GetTable(string tableName)
		{
			if (_Tables.TryGetValue(tableName, out var v))
				return v;
			throw new Exception();
		}
	}
}
