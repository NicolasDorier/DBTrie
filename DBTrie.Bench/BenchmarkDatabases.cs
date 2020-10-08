using BenchmarkDotNet.Attributes;
using DBTrie.Tests;
using LevelDB;
using LiteDB;
using rdb = RocksDbSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DBTrie.Bench
{
    [MarkdownExporterAttribute.GitHub]
    [RankColumn, MemoryDiagnoser]
    public class BenchmarkDatabases
    {
        private string folder;
        private DBTrieEngine trie;
        private DB ldb;
        private LiteDatabase litedb;
        private ILiteCollection<LiteDbEntity> litedbCol;
        private Transaction trx;
        private Table tbl;
        private int trieInsertCount = 0;
        private int ldbInsertCount = 0;
        private int litedbInsertCount = 0;
        private int trieGetCount = 0;
        private int ldbGetCount = 0;
        private int litedbGetCount = 0;
        private int trieDeleteCount = 0;
        private int ldbDeleteCount = 0;
        private int litedbDeleteCount = 0;
        private rdb.RocksDb rocksdb;

        private byte[] data = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

        [GlobalSetup]
        public void Setup()
        {
            folder = @"BenchData";
            Directory.CreateDirectory($@"BenchData");
            Directory.CreateDirectory($@"{folder}\trie");
            trie = DBTrieEngine.OpenFromFolder($@"{folder}\trie").Result;
            ldb = new DB(new Options { CreateIfMissing = true }, $@"{folder}\ldb");
            Directory.CreateDirectory($@"{folder}\litedb");
            litedb = new LiteDatabase(new ConnectionString() { Filename = $@"{folder}\litedb\db" });
            this.litedbCol = litedb.GetCollection<LiteDbEntity>("tbl");
            trx = trie.OpenTransaction().Result;
            tbl = trx.GetTable("tbl");
            rocksdb = rdb.RocksDb.Open(new rdb.DbOptions().SetCreateIfMissing(true), $@"{folder}\rocksdb");
        }

        [GlobalCleanup]
        public async Task Cleanup()
        {
            trx.Dispose();
            await trie.DisposeAsync();
            ldb.Dispose();
            litedb.Dispose();
            rocksdb.Dispose();
        }

        [Benchmark]
        public async Task TrieInsert()
        {
            for (int i = 0; i < 10; i++)
            {
                await tbl.Insert(BitConverter.GetBytes(trieInsertCount++), data);
            }
            await tbl.Commit();
        }

        [Benchmark]
        public void LeveldbInsert()
        {
            using (var batch = new WriteBatch())
            {
                for (int i = 0; i < 10; i++)
                {
                    batch.Put(new byte[] { 1 }.Concat(BitConverter.GetBytes(ldbInsertCount++)).ToArray(), data);
                }

                ldb.Write(batch);
            }
        }

        [Benchmark]
        public void RocksdbInsert()
        {
            using (var batch = new rdb.WriteBatch())
            {
                for (int i = 0; i < 10; i++)
                {
                    batch.Put(new byte[] { 1 }.Concat(BitConverter.GetBytes(ldbInsertCount++)).ToArray(), data);
                }

                rocksdb.Write(batch);
            }
        }

        [Benchmark]
        public void LitedbInsert()
        {
            var list = new List<LiteDbEntity>();

            for (int i = 0; i < 10; i++)
            {
                list.Add(new LiteDbEntity { Table = 1, Key = BitConverter.GetBytes(litedbInsertCount++), Value = data });
            }

            litedbCol.InsertBulk(list);
        }

        [Benchmark]
        public async Task TrieGet()
        {
            for (int i = 0; i < 10; i++)
            {
                var row = await tbl.Get(BitConverter.GetBytes(trieGetCount++));
                row?.Dispose();
            }
        }

        [Benchmark]
        public void LeveldbGet()
        {
            for (int i = 0; i < 10; i++)
            {
                ldb.Get(new byte[] { 1 }.Concat(BitConverter.GetBytes(ldbGetCount++)).ToArray());
            }
        }

        [Benchmark]
        public void RocksdbGet()
        {
            for (int i = 0; i < 10; i++)
            {
                rocksdb.Get(new byte[] { 1 }.Concat(BitConverter.GetBytes(ldbGetCount++)).ToArray());
            }
        }

        [Benchmark]
        public void LitedbGet()
        {
            for (int i = 0; i < 10; i++)
            {
                litedbCol.FindById(BitConverter.GetBytes(litedbGetCount++));
            }
        }

        [Benchmark]
        public async Task TrieDelete()
        {
            for (int i = 0; i < 10; i++)
            {
                await tbl.Delete(BitConverter.GetBytes(trieDeleteCount++));
            }

            await tbl.Commit();
        }

        [Benchmark]
        public void LeveldbDelete()
        {
            using (var batch = new WriteBatch())
            {
                for (int i = 0; i < 10; i++)
                {
                    batch.Delete(new byte[] { 1 }.Concat(BitConverter.GetBytes(ldbDeleteCount++)).ToArray());
                }

                ldb.Write(batch);
            }
        }

        [Benchmark]
        public void RocksdbDelete()
        {
            using (var batch = new rdb.WriteBatch())
            {
                for (int i = 0; i < 10; i++)
                {
                    batch.Delete(new byte[] { 1 }.Concat(BitConverter.GetBytes(ldbDeleteCount++)).ToArray());
                }

                rocksdb.Write(batch);
            }
        }

        [Benchmark]
        public void LitedbDelete()
        {
            for (int i = 0; i < 10; i++)
            {
                litedbCol.Delete(BitConverter.GetBytes(litedbDeleteCount++));
            }
        }
    }

    public class LiteDbEntity
    {
        public int Table { get; set; }

        [BsonId]
        public byte[] Key { get; set; }

        public byte[] Value { get; set; }
    }
}