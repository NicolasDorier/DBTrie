using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie
{
	public interface IRow : IDisposable
	{
		ReadOnlyMemory<byte> Key { get; }
		int ValueLength { get; }
		ValueTask<ReadOnlyMemory<byte>> ReadValue();
		ValueTask<ulong> ReadValueULong();
		ValueTask<string> ReadValueString();
	}
}
