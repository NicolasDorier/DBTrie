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
		public static async ValueTask Reserve(this IStorage storage, int additionalLength)
		{
			var location = storage.Length + additionalLength - 1;
			if (location >= storage.Length)
			{
				var oldPointer = storage.Length;
				var nothing = new byte[1];
				await storage.Write(location, nothing);
			}
		}
	}
}
