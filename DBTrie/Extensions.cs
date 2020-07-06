using System;
using System.Buffers;
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

		public static async ValueTask<long> WriteToEnd(this IStorage storage, ReadOnlyMemory<byte> input)
		{
			var oldPointer = storage.Length;
			await storage.Write(storage.Length, input);
			return oldPointer;
		}
		public static async ValueTask<long> Reserve(this IStorage storage, int length)
		{
			var oldPointer = storage.Length;
			var nothing = new byte[1];
			await storage.Write(storage.Length + length - 1, nothing);
			return oldPointer;
		}

		public static ulong BigEndianToULong(this ReadOnlySpan<byte> value)
		{
			return (ulong)(((ulong)value[0] << 56) + ((ulong)value[1] << 48) + ((ulong)value[2] << 40) + ((ulong)value[3] << 32) + ((ulong)value[4] << 24) + ((ulong)value[5] << 16) + ((ulong)value[6] << 8) + (ulong)value[7]);
		}
		public static ushort BigEndianToShort(this ReadOnlySpan<byte> span)
		{
			return (ushort)(span[0] << 8 | span[1]);
		}
		public static uint BigEndianToUInt(this ReadOnlySpan<byte> value)
		{
			return (uint)(value[0] << 24 | value[1] << 16 | value[2] << 8 | value[3]);
		}
		public static ReadOnlySpan<byte> AsReadOnlySpan(this byte[] arr)
		{
			return arr.AsSpan();
		}
		public static void ToBigEndian(this Span<byte> value, ulong val1)
		{
			value[0] = (byte)(val1 >> 56);
			value[1] = (byte)(val1 >> 48);
			value[2] = (byte)(val1 >> 40);
			value[3] = (byte)(val1 >> 32);
			value[4] = (byte)(val1 >> 24);
			value[5] = (byte)(val1 >> 16);
			value[6] = (byte)(val1 >> 8);
			value[7] = (byte)val1;
		}
		public static void ToBigEndianDynamic(this Span<byte> value, ulong val1)
		{
			Span<byte> v = stackalloc byte[8];
			v.ToBigEndian(val1);
			v.Slice(8 - Sizes.DefaultPointerLen).CopyTo(value);
		}
		public static void ToBigEndian(this Span<byte> value, uint val1)
		{
			value[0] = (byte)(val1 >> 24);
			value[1] = (byte)(val1 >> 16);
			value[2] = (byte)(val1 >> 8);
			value[3] = (byte)(val1);
		}
		public static void ToBigEndian(this Span<byte> value, ushort val1)
		{
			value[0] = (byte)(val1 >> 8);
			value[1] = (byte)(val1);
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
		public static IMemoryOwner<byte> RentExact(this MemoryPool<byte> memoryPool, int length)
		{
			var rented = memoryPool.Rent(length);
			return rented.Slice(0, length);
		}
	}
}
