using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie
{
	internal static class Extensions
	{
		public static ReadOnlyMemory<byte> ToReadOnly(this Memory<byte> memory)
		{
			return memory;
		}
		public static ulong BigEndianToLongDynamic(this ReadOnlySpan<byte> span)
		{
			ulong res = 0;
			int vl = span.Length;
			for (int i = 0; i < vl; i++)
			{
				res += (ulong)span[i] << ((vl - 1 - i) * 8);
			}
			return res;
		}

		public static ulong ReadUInt64BigEndian(this ReadOnlySpan<byte> value)
		{
			return BinaryPrimitives.ReadUInt64BigEndian(value);
		}
		public static ushort ReadUInt16BigEndian(this ReadOnlySpan<byte> span)
		{
			return BinaryPrimitives.ReadUInt16BigEndian(span);
		}
		public static uint ReadUInt32BigEndian(this ReadOnlySpan<byte> value)
		{
			return BinaryPrimitives.ReadUInt32BigEndian(value);
		}
		public static ReadOnlySpan<byte> AsReadOnlySpan(this byte[] arr)
		{
			return arr.AsSpan();
		}
		public static void ToBigEndian(this Span<byte> value, ulong val1)
		{
			BinaryPrimitives.WriteUInt64BigEndian(value, val1);
		}
		public static void ToBigEndianDynamic(this Span<byte> value, ulong val1)
		{
			Span<byte> v = stackalloc byte[8];
			v.ToBigEndian(val1);
			v.Slice(8 - Sizes.DefaultPointerLen).CopyTo(value);
		}
		public static void ToBigEndian(this Span<byte> value, uint val1)
		{
			BinaryPrimitives.WriteUInt32BigEndian(value, val1);
		}
		public static void ToBigEndian(this Span<byte> value, ushort val1)
		{
			BinaryPrimitives.WriteUInt16BigEndian(value, val1);
		}
		internal class SlicedMemoryOwner : IMemoryOwner<byte>
		{
			readonly IMemoryOwner<byte> memoryOwner;
			readonly Memory<byte> slice;

			public SlicedMemoryOwner(IMemoryOwner<byte> memoryOwner, Memory<byte> slice)
			{
				this.memoryOwner = memoryOwner;
				this.slice = slice;
			}

			public Memory<byte> Memory => slice;

			public void Dispose()
			{
				this.memoryOwner.Dispose();
			}
		}
		public static IMemoryOwner<byte> Slice(this IMemoryOwner<byte> memoryOwner, int start, int length)
		{
			if (memoryOwner == null)
				throw new ArgumentNullException(nameof(memoryOwner));
			if (start == 0 && length == memoryOwner.Memory.Length)
				return memoryOwner;
			var sliced = memoryOwner.Memory.Slice(start, length);
			return new SlicedMemoryOwner(memoryOwner, sliced);
		}
	}
}
