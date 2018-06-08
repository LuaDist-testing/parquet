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
      assert.is_not_nil(schema:findField('point'))
      assert.is_not_nil(schema:findField('point,indices'))
      assert.is_not_nil(schema:findField('point,indices,list'))
      assert.is_not_nil(schema:findField('point,indices,list,element'))
    end)
    
    it('regression test issue#19', function()
      -- https://github.com/BixData/lua-parquet/issues/19
      local reader = ParquetReader.openFile('spec-fixtures/model2-data-part-00003.parquet')
      local schema = reader:getSchema()
      assert.is_not_nil(schema.fields.id)
      assert.is_not_nil(schema.fields.point)
      assert.is_not_nil(schema.fields.point.fields.type)
      assert.is_not_nil(schema.fields.point.fields.size)
      assert.is_not_nil(schema.fields.point.fields.indices)
      assert.is_not_nil(schema.fields.point.fields.indices.fields.list)
      assert.is_not_nil(schema.fields.point.fields.indices.fields.list.fields.element)
      assert.is_not_nil(schema.fields.point.fields.values)
      assert.is_not_nil(schema.fields.point.fields.values.fields.list)
      assert.is_not_nil(schema.fields.point.fields.values.fields.list.fields.element)
    end)
    
  end)
  
end)
