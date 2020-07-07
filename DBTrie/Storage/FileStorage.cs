using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie.Storage
{
	public class FileStorage : IStorage, IAsyncDisposable
	{
		string _fileName;
		int _fileStreamBufferSize = 8192;
		private FileStream _fsData;
		//private readonly FileStream _fsRollback;
		//private readonly FileStream _fsRollbackHelper;

		public FileStorage(string fileName)
		{
			if (fileName == null)
				throw new ArgumentNullException(nameof(fileName));
			_fileName = fileName;
			this._fsData = OpenFile();
			//this._fsRollback = new FileStream(this._fileName + ".rol", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, _fileStreamBufferSize, FileOptions.WriteThrough);
			//this._fsRollbackHelper = new FileStream(this._fileName + ".rhp", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, _fileStreamBufferSize, FileOptions.WriteThrough);
		}

		private FileStream OpenFile()
		{
			return new FileStream(this._fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, _fileStreamBufferSize, FileOptions.WriteThrough);
		}

		public long Length => _fsData.Length;

		public async ValueTask Read(long offset, Memory<byte> output)
		{
			_fsData.Seek(offset, SeekOrigin.Begin);
			var len = await _fsData.ReadAsync(output);
			output.Span.Slice(len).Fill(0);
		}

		public async ValueTask Write(long offset, ReadOnlyMemory<byte> input)
		{
			_fsData.Seek(offset, SeekOrigin.Begin);
			await _fsData.WriteAsync(input);
		}

		public ValueTask DisposeAsync()
		{
			return _fsData.DisposeAsync();
		}

		public async ValueTask Rezise(long newLength)
		{
			_fsData.Seek(newLength - 1, SeekOrigin.Begin);
			var b = new byte[1];
			await _fsData.WriteAsync(b);
		}

		public async ValueTask Flush()
		{
			await _fsData.FlushAsync();
		}
	}
}
