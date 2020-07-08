using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie
{
	public static class PublicExtensions
	{
		public static async ValueTask<T[]> ToArrayAsync<T>(this IAsyncEnumerable<T> items)
		{
			List<T> list = new List<T>();
			await foreach (var i in items)
			{
				list.Add(i);
			}
			return list.ToArray();
		}
		public static ArraySegment<byte> GetUnderlyingArraySegment(this ReadOnlyMemory<byte> bytes)
		{
			if (!MemoryMarshal.TryGetArray(bytes, out var arraySegment)) throw new NotSupportedException("This Memory does not support exposing the underlying array.");
			return arraySegment;
		}
	}
}
