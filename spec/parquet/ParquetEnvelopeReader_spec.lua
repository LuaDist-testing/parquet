local ParquetEnvelopeReader = require "parquet.ParquetEnvelopeReader"
--local thrift_print_r = require "thrift.print_r"

describe('ParquetEnvelopeReader', function()

  describe('model3.snappy.parquet', function()
  
    it('reads metadata', function()
      local envelopeReader = ParquetEnvelopeReader.openFile('spec-fixtures/model3.snappy.parquet')
      envelopeReader:readHeader()
      local metadata = envelopeReader:readFooter()
      
      assert.equals('parquet-mr version 1.8.2 (build c6522788629e590a53eb79874b95f6c3ff11f16c)', metadata.created_by)
      assert.same({}, metadata.row_groups)
      assert.equals(1, metadata.version)
      assert.equals('0', metadata.num_rows:toString())
      
      local kv = metadata.key_value_metadata[1]
      assert.equals('org.apache.spark.sql.parquet.row.metadata', kv.key)
      assert.is_true(string.match(kv.value, '"type":"udt","class":"org.apache.spark.mllib.linalg.VectorUDT"') ~= nil)
    end)
    
  end)
  
end)
