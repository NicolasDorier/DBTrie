using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie.TrieModel
{
	//1byte - protocol, FullKeyLen (2 bytes), FullValueLen (4 bytes),[Reserved Space For Update- 4 bytes],FullKey,FullValue
	internal class LTrieValue : IRow
	{
		LTrie trie;
		public LTrieValue(LTrie trie, IMemoryOwner<byte> key, int valueLength)
		{
			this.trie = trie;
			this.key = key;
			this.ValueLength = valueLength;
		}
		readonly IMemoryOwner<byte> key;
		IMemoryOwner<byte>? value;
		public ReadOnlyMemory<byte> Key => key.Memory;
		/// <summary>
		/// If 1, then ValueMaxLength is saved on a 4 bytes field
		/// If 0, then ValueMaxLength is not serialized but equals to ValueLength
		/// </summary>
		public byte Protocol;
		public int KeyLength => Key.Length;
		public int ValueLength { get; }
		public long ValuePointer;
		public int ValueMaxLength;
		internal long Pointer;
		public async ValueTask<ReadOnlyMemory<byte>> ReadValue()
		{
			if (value is IMemoryOwner<byte> v)
				return v.Memory;
			v = trie.MemoryPool.Rent(ValueLength);
			await trie.Storage.Read(ValuePointer, v.Memory.Slice(0, ValueLength));
			v = v.Slice(0, ValueLength);
			value = v;
			return v.Memory;
		}
		public async ValueTask<string> ReadValueString()
		{
			return Encoding.UTF8.GetString((await ReadValue()).Span);
		}
		public async ValueTask<ulong> ReadValueULong()
		{
			if (ValueLength != 8)
				throw new InvalidOperationException("This value is not ulong");
			return (await ReadValue()).Span.ReadUInt64BigEndian();
		}

		public static int WriteToSpan(Span<byte> output, ReadOnlySpan<byte> key, ReadOnlySpan<byte> newValue)
		{
			int i = 0;
			output[i] = 0;
			i++;
			output.Slice(i).ToBigEndian((ushort)key.Length);
			i += 2;
			output.Slice(i).ToBigEndian((uint)newValue.Length);
			i += 4;
			key.CopyTo(output.Slice(i));
			i += key.Length;
			newValue.CopyTo(output.Slice(i));
			i += newValue.Length;
			return i;
		}
		public int WriteToSpan(Span<byte> output, ReadOnlySpan<byte> newValue)
		{
			if (newValue.Length > ValueMaxLength)
				throw new InvalidOperationException("The new value is too big to replace the old record");
			var protocol = ValueMaxLength - 4 < newValue.Length ? 0 : 1;
			int i = 0;
			output[i] = (byte)protocol;
			i++;
			output.Slice(i).ToBigEndian((ushort)KeyLength);
			i += 2;
			output.Slice(i).ToBigEndian((uint)newValue.Length);
			i += 4;
			if (protocol == 1)
			{
				output.Slice(i).ToBigEndian((uint)ValueMaxLength);
				i += 4;
			}
			Key.Span.CopyTo(output.Slice(i));
			i += KeyLength;
			newValue.CopyTo(output.Slice(i));
			i += newValue.Length;
			return i;
		}

		public int Size => (Protocol == 0 ? 7 : 11) + KeyLength + ValueLength;
		public static int GetSize(int keySize, int valueSize, int valueMaxSize)
		{
			return ((valueSize == valueMaxSize) ? 7 : 11) + keySize + valueSize;
		}

		public void Dispose()
		{
			this.key.Dispose();
			this.value?.Dispose();
		}
	}
}
