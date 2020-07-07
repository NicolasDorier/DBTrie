using DBTrie.Storage;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Drawing;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie.TrieModel
{
	public class LTrie
	{
		internal MemoryPool<byte> MemoryPool { get; }
		internal IStorage Storage { get; }
		public StorageWriter StorageWriter { get; }

		LTrieRootNode? _RootNode;
		public LTrieRootNode RootNode
		{
			get
			{
				if (_RootNode is LTrieRootNode n)
					return n;
				throw new InvalidOperationException("ReadRootNode is still not called");
			}
		}
		public bool ConsistencyCheck { get; set; }
		public void ActivateCache()
		{
			GenerationNodeCache = new GenerationNodeCache();
		}
		public GenerationNodeCache? GenerationNodeCache { get; private set; }

		internal async ValueTask<LTrieKidRecord> ReadRecord(long pointer)
		{
			//1byte - protocol, FullKeyLen (2 bytes), FullValueLen (4 bytes),[Reserved Space For Update- 4 bytes],FullKey,FullValue
			//1 + 2 + 4 + 4 + 100 = 111
			int readLen = 256;
			var owner = MemoryPool.Rent(readLen);
			var memory = owner.Memory.Slice(0, readLen);
			await Storage.Read(pointer, memory);
			var protocol = memory.Span[0];
			var headerSize = protocol == 0 ? 7 : 11;
			ushort keySize = memory.ToReadOnly().Span.Slice(1, 2).BigEndianToShort();
			if (keySize > (readLen - headerSize))
			{
				readLen = keySize + headerSize;
				owner.Dispose();
				owner = MemoryPool.Rent(readLen);
				memory = owner.Memory.Slice(0, readLen);
				await Storage.Read(pointer, memory);
			}
			bool nullValue = (memory.Span[3] & 0x80) != 0;
			int valueSize = (int)(nullValue ? 0U : memory.ToReadOnly().Span.Slice(3).BigEndianToUInt());
			return new LTrieKidRecord(owner.Slice(headerSize, keySize))
			{
				Protocol = protocol,
				Pointer = pointer,
				ValueLength = valueSize,
				ValuePointer = pointer + headerSize + keySize,
				ValueMaxLength = protocol == 0 ? valueSize : (int)memory.Slice(7).ToReadOnly().Span.BigEndianToUInt()
			};
		}

		internal async ValueTask<bool> OverwriteKidValue(LTrieKid kid, ReadOnlyMemory<byte> value)
		{
			if (kid.LinkToNode)
				return false;
			using var record = await ReadRecord(kid.RecordPointer);
			if (record.ValueMaxLength >= value.Length)
			{
				using var owner = this.MemoryPool.Rent(LTrieKidRecord.GetRecordSize(record.Key, value));
				var len = record.WriteToSpan(owner.Memory.Span, value.Span);
				await Storage.Write(kid.RecordPointer, owner.Memory.Slice(0, len));
				return true;
			}
			return false;
		}

		internal IMemoryOwner<byte> GetNameAsBytes(string str)
		{
			var bytes = Encoding.UTF8.GetByteCount(str, 0, str.Length);
			var owner = MemoryPool.Rent(bytes);
			Encoding.UTF8.GetBytes(str.AsSpan(), owner.Memory.Span);
			return owner.Slice(0, bytes);
		}

		public LTrie(IStorage storage, MemoryPool<byte>? memoryPool = null)
		{
			if (storage == null)
				throw new ArgumentNullException(nameof(storage));
			memoryPool ??= MemoryPool<byte>.Shared;
			MemoryPool = memoryPool;
			Storage = storage;
			StorageWriter = new StorageWriter(memoryPool, storage);
		}


		public async ValueTask<LTrieRootNode> ReadRootNode()
		{
			if (_RootNode is LTrieRootNode r)
				return r;
			using var owner = MemoryPool.Rent(Sizes.RootSize);
			await Storage.Read(0, owner.Memory);
			var memory = owner.Memory.ToReadOnly();
			_RootNode = LTrieRootNode.ReadFromMemory(this, memory);
			return _RootNode;
		}
	}
}
