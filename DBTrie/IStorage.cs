using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie
{
	public interface IStorage : IAsyncDisposable
	{
		ValueTask Read(long offset, Memory<byte> output);
		ValueTask Write(long offset, ReadOnlyMemory<byte> input);
		ValueTask Flush();
		public long Length { get; }
	}
}
