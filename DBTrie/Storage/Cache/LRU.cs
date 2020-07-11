using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace DBTrie.Storage.Cache
{
	internal class LRU<T>
	{
		public int Count => hashmap.Count;
		LinkedList<T> list = new LinkedList<T>();
		Dictionary<T, LinkedListNode<T>> hashmap = new Dictionary<T, LinkedListNode<T>>();
		public void Accessed(T page)
		{
			if (hashmap.TryGetValue(page, out var node))
			{
				list.Remove(node);
				list.AddLast(node);
			}
			else
			{
				node = list.AddLast(page);
				hashmap.Add(page, node);
			}
			Debug.Assert(hashmap.Count == list.Count);
		}
		public bool TryPop(out T page)
		{
			if (list.Count == 0)
			{
				page = default!;
				return false;
			}
			page = list.First.Value;
			hashmap.Remove(page);
			list.RemoveFirst();
			Debug.Assert(hashmap.Count == list.Count);
			return true;
		}

		public void Push(T page)
		{
			var node = list.AddFirst(page);
			hashmap.Add(page, node);
			Debug.Assert(hashmap.Count == list.Count);
		}

		public void Remove(T page)
		{
			if (hashmap.TryGetValue(page, out var node))
			{
				list.Remove(node);
				hashmap.Remove(page);
			}
			Debug.Assert(hashmap.Count == list.Count);
		}
	}
}
