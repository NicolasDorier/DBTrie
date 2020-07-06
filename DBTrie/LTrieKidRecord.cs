using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace DBTrie
{
	//1byte - protocol, FullKeyLen (2 bytes), FullValueLen (4 bytes),[Reserved Space For Update- 4 bytes],FullKey,FullValue
	internal class LTrieKidRecord : IDisposable
	{
		public LTrieKidRecord(IMemoryOwner<byte> key)
		{
			this.key = key;
		}
		readonly IMemoryOwner<byte> key;
		public Memory<byte> Key => key.Memory;
		/// <summary>
		/// If 1, then ValueMaxLength is saved on a 4 bytes field
		/// If 0, then ValueMaxLength is not serialized but equals to ValueLength
		/// </summary>
		public byte Protocol;
		public int KeyLength => Key.Length;
		public int ValueLength;
		public long ValuePointer;
		public int ValueMaxLength;
		internal long Pointer;

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

		public static int GetRecordSize(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
		{
			return 11 + key.Length + value.Length;
		}

		public void Dispose()
		{
			this.key.Dispose();
		}
	}
}
