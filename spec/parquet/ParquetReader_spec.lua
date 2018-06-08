local ParquetReader = require "parquet.ParquetReader"
--local thrift_print_r = require "thrift.print_r"

describe('ParquetReader', function()

  describe('model3.snappy.parquet', function()
  
    it('reads metadata', function()
      local reader = ParquetReader.openFile('spec-fixtures/model3.snappy.parquet')
      local metadata = reader:getMetadata()
      assert.is_not_nil(metadata['org.apache.spark.sql.parquet.row.metadata'])
    end)
    
    it('reads rowCount', function()
      local reader = ParquetReader.openFile('spec-fixtures/model3.snappy.parquet')
      local rowCount = reader:getRowCount()
      assert.equals(0, rowCount:toInt())
    end)
    
    it('reads schema', function()
      local reader = ParquetReader.openFile('spec-fixtures/model3.snappy.parquet')
      local schema = reader:getSchema()
      assert.is_not_nil(schema)
    end)
    
  end)
  
end)
