using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie.Storage
{
	public interface IStorages
	{
		ValueTask<IStorage> OpenStorage(string name);
	}
}
