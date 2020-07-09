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
		private FileStream _fsData;

		public FileStorage(string fileName, int bufferSize = Sizes.DefaultPageSize)
		{
			if (fileName == null)
				throw new ArgumentNullException(nameof(fileName));
			_fileName = fileName;
			this._fsData = new FileStream(this._fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, bufferSize, FileOptions.Asynchronous);
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

		public async ValueTask DisposeAsync()
		{
			await _fsData.FlushAsync();
			await _fsData.DisposeAsync();
		}


		public async ValueTask Flush()
		{
			await _fsData.FlushAsync();
		}

		public async ValueTask Resize(long newSize)
		{
			if (newSize > _fsData.Length)
			{
				var nothing = new byte[1];
				_fsData.Seek(newSize - 1, SeekOrigin.Begin);
				await _fsData.WriteAsync(nothing);
			}
			else if (newSize < _fsData.Length)
			{
				await _fsData.FlushAsync();
				_fsData.SetLength(newSize);
			}
		}
	}
}
