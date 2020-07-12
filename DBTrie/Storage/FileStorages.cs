using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie.Storage
{
	public class FileStorages : IStorages
	{
		private readonly string folderName;
		public FileStorages(string folderName)
		{
			if (folderName == null)
				throw new ArgumentNullException(nameof(folderName));
			this.folderName = folderName;
		}

		public ValueTask Copy(string src, string dest)
		{
			dest = Path.Combine(folderName, dest);
			src = Path.Combine(folderName, src);
			if (File.Exists(dest))
				File.Delete(dest);
			File.Copy(src, dest);
			return default;
		}

		public ValueTask Delete(string name)
		{
			var file = Path.Combine(folderName, name);
			File.Delete(file);
			// Old DBReeze stuff
			File.Delete($"{file}.rhp");
			File.Delete($"{file}.rol");
			return default;
		}

		public ValueTask<bool> Exists(string name)
		{
			return new ValueTask<bool>(File.Exists(Path.Combine(folderName, name)));
		}

		public ValueTask Move(string src, string dest)
		{
			dest = Path.Combine(folderName, dest);
			src = Path.Combine(folderName, src);
			if (File.Exists(dest))
				File.Delete(dest);
			File.Move(src, dest);
			return default;
		}

		public ValueTask<IStorage> OpenStorage(string name)
		{
			if (name == null)
				throw new ArgumentNullException(nameof(name));
			var filePath = Path.Combine(folderName, name);
			try
			{
				return new ValueTask<IStorage>(new FileStorage(filePath));
			}
			catch (FileNotFoundException)
			{
				File.Create(filePath).Close();
			}
			return new ValueTask<IStorage>(new FileStorage(filePath));
		}
	}
}
