namespace Parquet.Data
{
   /// <summary>
   /// Maps to Parquet uuid type
   /// </summary>
   public class UuidDataField : DataField
   {
      /// <summary>
      /// Constructs class instance
      /// </summary>
      /// <param name="name">The name of the column</param>
      /// <param name="hasNulls">Is 'Guid?'</param>
      /// <param name="isArray">Indicates whether this field is repeatable.</param>
      public UuidDataField(string name, bool hasNulls = true, bool isArray = false)
         : base(name, DataType.Uuid, hasNulls, isArray) { }

   }
}