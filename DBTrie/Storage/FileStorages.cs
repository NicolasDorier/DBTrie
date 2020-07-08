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
