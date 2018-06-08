local parquet = require 'parquet'
--local thrift_print_r = require 'thrift.print_r'

local filename = 'model2-data-part-00003.parquet'

--[[
KMeans model, trained and exported uncompressed with:

$ docker run -it gettyimages/spark bin/spark-shell --conf spark.sql.parquet.compression.codec=uncompressed

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans

var v1 = Vectors.dense(Array[Double](1,2,3))
var v2 = Vectors.dense(Array[Double](5,6,7))
var data = sc.parallelize(Array(v1,v2))
var model = KMeans.train(data, k=1, maxIterations=1)
model.save(sc, "model2")
--]]

--[[
The expected content of this parquet file is established using parquet-mr/parquet-tools.

1. Install per https://community.hortonworks.com/articles/73952/build-and-use-parquet-tools-to-read-parquet-files.html
2. $ java -jar target/parquet-tools-1.9.0.jar head lua-parquet/spec-fixtures/model2-data-part-00003.parquet
id = 0
point:
.type = 1
.values:
..list:
...element = 3.0
..list:
...element = 4.0
..list:
...element = 5.0
--]]

describe(filename, function()

  it('readTestFile', function()
    local reader = parquet.ParquetReader.openFile('spec-fixtures/' .. filename)
    assert.equal(1, reader:getRowCount():toInt())
    assert.is_not_nil(reader:getMetadata())
    assert.is_not_nil(reader:getMetadata()['org.apache.spark.sql.parquet.row.metadata'])
  
    local schema = reader:getSchema()
    assert.equals(10, #schema.fieldList)
    assert.is_not_nil(schema.fields.id)
    assert.is_not_nil(schema.fields.point)
  
    do
      local c = schema.fields.id
      assert.equal('id', c.name)
      assert.equal('INT32', c.primitiveType)
      assert.equal(nil, c.originalType)
      assert.same({'id'}, c.path)
      assert.equal('REQUIRED', c.repetitionType)
      assert.equal('PLAIN', c.encoding)
      assert.equal('UNCOMPRESSED', c.compression)
      assert.equal(0, c.rLevelMax)
      assert.equal(0, c.dLevelMax)
      assert.equal(false, not not c.isNested)
      assert.equal(nil, c.fieldCount)
    end
  
    do
      local c = schema.fields.point
      assert.equal('point', c.name)
      assert.equal(nil, c.primitiveType)
      assert.equal(nil, c.originalType)
      assert.same({'point'}, c.path)
      assert.equal('OPTIONAL', c.repetitionType)
      assert.equal(nil, c.encoding)
      assert.equal(nil, c.compression)
      assert.equal(0, c.rLevelMax)
      assert.equal(1, c.dLevelMax)
      assert.equal(true, not not c.isNested)
      assert.equal(4, c.fieldCount)
    end

    do
      local cursor = reader:getCursor()
      local row = cursor:next()
      assert.equal(0, row.id)
      assert.same({{element=3}, {element=4}, {element=5}}, row.point.values.list)
    end
      
    reader:close()
  end)
  
  it('readTestFile, preloaded into a string buffer', function()
    local f = io.open('spec-fixtures/' .. filename, 'r')
    local buffer = f:read('*a')
    f:close()
    
    local reader = parquet.ParquetReader.openString(buffer)
    assert.equal(1, reader:getRowCount():toInt())
    assert.is_not_nil(reader:getMetadata())
    assert.is_not_nil(reader:getMetadata()['org.apache.spark.sql.parquet.row.metadata'])
  
    local schema = reader:getSchema()
    assert.equals(10, #schema.fieldList)
    assert.is_not_nil(schema.fields.id)
    assert.is_not_nil(schema.fields.point)
  
    do
      local c = schema.fields.id
      assert.equal('id', c.name)
      assert.equal('INT32', c.primitiveType)
      assert.equal(nil, c.originalType)
      assert.same({'id'}, c.path)
      assert.equal('REQUIRED', c.repetitionType)
      assert.equal('PLAIN', c.encoding)
      assert.equal('UNCOMPRESSED', c.compression)
      assert.equal(0, c.rLevelMax)
      assert.equal(0, c.dLevelMax)
      assert.equal(false, not not c.isNested)
      assert.equal(nil, c.fieldCount)
    end
  
    do
      local c = schema.fields.point
      assert.equal('point', c.name)
      assert.equal(nil, c.primitiveType)
      assert.equal(nil, c.originalType)
      assert.same({'point'}, c.path)
      assert.equal('OPTIONAL', c.repetitionType)
      assert.equal(nil, c.encoding)
      assert.equal(nil, c.compression)
      assert.equal(0, c.rLevelMax)
      assert.equal(1, c.dLevelMax)
      assert.equal(true, not not c.isNested)
      assert.equal(4, c.fieldCount)
    end

    do
      local cursor = reader:getCursor()
      local row = cursor:next()
      assert.equal(0, row.id)
      assert.same({{element=3}, {element=4}, {element=5}}, row.point.values.list)
    end
      
    reader:close()
  end)
  
end)
