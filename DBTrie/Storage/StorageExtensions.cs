using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie.Storage
{
	public static class StorageExtensions
	{
		public static async ValueTask<long> WriteToEnd(this IStorage storage, ReadOnlyMemory<byte> input)
		{
			var oldPointer = storage.Length;
			await storage.Write(storage.Length, input);
			return oldPointer;
		}
	}
}
