using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie.Storage
{
	public class StorageWriter
	{
		public IStorage Storage { get; }
		public MemoryPool<byte> MemoryPool { get; }

		public StorageWriter(MemoryPool<byte> memoryPool, IStorage storage)
		{
			if (memoryPool == null)
				throw new ArgumentNullException(nameof(memoryPool));
			this.MemoryPool = memoryPool;
			this.Storage = storage;
		}
		internal async ValueTask WritePointer(long position, long pointer)
		{
			using var owner = MemoryPool.Rent(Sizes.DefaultPointerLen);
			owner.Memory.Span.ToBigEndianDynamic((ulong)pointer);
			await Storage.Write(position, owner.Memory.Slice(0, Sizes.DefaultPointerLen));
		}
		internal async ValueTask WriteExternalLink(long position, byte label, bool linkToNode, long pointer)
		{
			using var owner = MemoryPool.Rent(2 + Sizes.DefaultPointerLen);
			owner.Memory.Span[0] = label;
			owner.Memory.Span[1] = (byte)(linkToNode ? 0 : 1);
			owner.Memory.Span.Slice(2).ToBigEndianDynamic((ulong)pointer);
			await Storage.Write(position, owner.Memory.Slice(0, Sizes.ExternalLinkLength));
		}

		internal async ValueTask WriteLong(long position, long value)
		{
			using var owner = MemoryPool.Rent(8);
			owner.Memory.Span.ToBigEndian((ulong)(value));
			await Storage.Write(position, owner.Memory.Slice(0, 8));
		}
	}
}
