﻿using System;
using System.IO;
using System.Linq;
using NetBox.Extensions;
using Parquet.Data;
using Xunit;

namespace Parquet.Test.Serialisation
{
   public class ParquetConvertTest : TestBase
   {
      [Fact]
      public void Serialise_deserialise_all_types()
      {
         DateTime now = DateTime.Now;

         SimpleStructure[] structures = Enumerable
            .Range(0, 10)
            .Select(i => new SimpleStructure
            {
               Id = i,
               NullableId = (i % 2 == 0) ? new int?() : new int?(i),
               Name = $"row {i}",
               Date = now.AddDays(i).RoundToSecond().ToUniversalTime(),
               Guid = Guid.NewGuid()
            })
            .ToArray();

         using (var ms = new MemoryStream())
         {
            Schema schema = ParquetConvert.Serialize(structures, ms, compressionMethod: CompressionMethod.Snappy);

            ms.Position = 0;

            SimpleStructure[] structures2 = ParquetConvert.Deserialize<SimpleStructure>(ms);

            for(int i = 0; i < 10; i++)
            {
               Assert.Equal(structures[i].Id, structures2[i].Id);
               Assert.Equal(structures[i].NullableId, structures2[i].NullableId);
               Assert.Equal(structures[i].Name, structures2[i].Name);
               Assert.Equal(structures[i].Date, structures2[i].Date);
               Assert.Equal(structures[i].Guid, structures2[i].Guid);
            }

         }
      }

      public class SimpleStructure
      {
         public int Id { get; set; }

         public int? NullableId { get; set; }

         public string Name { get; set; }

         public DateTimeOffset Date { get; set; }

         public Guid Guid { get; set; }
      }

   }
}