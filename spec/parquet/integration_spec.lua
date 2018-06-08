local parquet = require 'parquet'
--local thrift_print_r = require 'thrift.print_r'

--local TEST_NUM_ROWS = 10000

describe('fruits.parquet', function()

  it('readTestFile', function()
    local reader = parquet.ParquetReader.openFile('spec-fixtures/fruits.parquet')
--    assert.equal(TEST_NUM_ROWS * 4, reader:getRowCount():toInt())
    assert.same({myuid='420', fnord='dronf'}, reader:getMetadata())
  
    local schema = reader:getSchema()
    assert.equals(9, #schema.fieldList)
    assert.is_not_nil(schema.fields.name)
    assert.is_not_nil(schema.fields.stock)
    assert.is_not_nil(schema.fields.stock.fields.quantity)
    assert.is_not_nil(schema.fields.stock.fields.warehouse)
    assert.is_not_nil(schema.fields.price)
  
    do
      local c = schema.fields.name
      assert.equal('name', c.name)
      assert.equal('BYTE_ARRAY', c.primitiveType)
      assert.equal('UTF8', c.originalType)
      assert.same({'name'}, c.path)
      assert.equal('REQUIRED', c.repetitionType)
      assert.equal('PLAIN', c.encoding)
      assert.equal('UNCOMPRESSED', c.compression)
      assert.equal(0, c.rLevelMax)
      assert.equal(0, c.dLevelMax)
      assert.equal(false, not not c.isNested)
      assert.equal(nil, c.fieldCount)
    end
  
    do
      local c = schema.fields.stock
      assert.equal('stock', c.name)
      assert.equal(nil, c.primitiveType)
      assert.equal(nil, c.originalType)
      assert.same({'stock'}, c.path)
      assert.equal('REPEATED', c.repetitionType)
      assert.equal(nil, c.encoding)
      assert.equal(nil, c.compression)
      assert.equal(1, c.rLevelMax)
      assert.equal(1, c.dLevelMax)
      assert.equal(true, not not c.isNested)
      assert.equal(2, c.fieldCount)
    end
  
    do
      local c = schema.fields.stock.fields.quantity
      assert.equal('quantity', c.name)
      assert.equal('INT64', c.primitiveType)
      assert.equal(nil, c.originalType)
      assert.same({'stock','quantity'}, c.path)
      assert.equal('REPEATED', c.repetitionType)
      assert.equal('PLAIN', c.encoding)
      assert.equal('UNCOMPRESSED', c.compression)
      assert.equal(2, c.rLevelMax)
      assert.equal(2, c.dLevelMax)
      assert.equal(false, not not c.isNested)
      assert.equal(nil, c.fieldCount)
    end
  
    do
      local c = schema.fields.stock.fields.warehouse
      assert.equal('warehouse', c.name)
      assert.equal('BYTE_ARRAY', c.primitiveType)
      assert.equal('UTF8', c.originalType)
      assert.same({'stock','warehouse'}, c.path)
      assert.equal('REQUIRED', c.repetitionType)
      assert.equal('PLAIN', c.encoding)
      assert.equal('UNCOMPRESSED', c.compression)
      assert.equal(1, c.rLevelMax)
      assert.equal(1, c.dLevelMax)
      assert.equal(false, not not c.isNested)
      assert.equal(nil, c.fieldCount)
    end
  
    do
      local c = schema.fields.price
      assert.equal('price', c.name)
      assert.equal('DOUBLE', c.primitiveType)
      assert.equal(nil, c.originalType)
      assert.same({'price'}, c.path)
      assert.equal('REQUIRED', c.repetitionType)
      assert.equal('PLAIN', c.encoding)
      assert.equal('UNCOMPRESSED', c.compression)
      assert.equal(0, c.rLevelMax)
      assert.equal(0, c.dLevelMax)
      assert.equal(false, not not c.isNested)
      assert.equal(nil, c.fieldCount)
    end
  
--    do
--      let cursor = reader.getCursor()
--      for (let i = 0 i < TEST_NUM_ROWS ++i) {
--        assert.deepEqual(await cursor.next(), {
--          name: 'apples',
--          quantity: 10,
--          price: 2.6,
--          date: new Date(TEST_VTIME + 1000 * i),
--          stock: [
--            { quantity: [10], warehouse: "A" },
--            { quantity: [20], warehouse: "B" }
--          ],
--          colour: [ 'green', 'red' ]
--        })
--
--        assert.deepEqual(await cursor.next(), {
--          name: 'oranges',
--          quantity: 20,
--          price: 2.7,
--          date: new Date(TEST_VTIME + 2000 * i),
--          stock: [
--            { quantity: [50, 75], warehouse: "X" }
--          ],
--          colour: [ 'orange' ]
--        })
--
--        assert.deepEqual(await cursor.next(), {
--          name: 'kiwi',
--          price: 4.2,
--          date: new Date(TEST_VTIME + 8000 * i),
--          stock: [
--            { quantity: [420], warehouse: "f" },
--            { quantity: [20], warehouse: "x" }
--          ],
--          colour: [ 'green', 'brown' ],
--          meta_json: { expected_ship_date: TEST_VTIME }
--        })
--
--        assert.deepEqual(await cursor.next(), {
--          name: 'banana',
--          price: 3.2,
--          date: new Date(TEST_VTIME + 6000 * i),
--          colour: [ 'yellow' ],
--          meta_json: { shape: 'curved' }
--        })
--      }
--
--      assert.equal(await cursor.next(), null)
--    end
--
--    do
--      let cursor = reader.getCursor(['name'])
--      for (let i = 0 i < TEST_NUM_ROWS ++i) {
--        assert.deepEqual(await cursor.next(), { name: 'apples' })
--        assert.deepEqual(await cursor.next(), { name: 'oranges' })
--        assert.deepEqual(await cursor.next(), { name: 'kiwi' })
--        assert.deepEqual(await cursor.next(), { name: 'banana' })
--      }
--
--      assert.equal(await cursor.next(), null)
--    end
--
--    do
--      let cursor = reader.getCursor(['name', 'quantity'])
--      for (let i = 0 i < TEST_NUM_ROWS ++i) {
--        assert.deepEqual(await cursor.next(), { name: 'apples', quantity: 10 })
--        assert.deepEqual(await cursor.next(), { name: 'oranges', quantity: 20 })
--        assert.deepEqual(await cursor.next(), { name: 'kiwi' })
--        assert.deepEqual(await cursor.next(), { name: 'banana' })
--      }
--
--      assert.equal(await cursor.next(), null)
--    end
  
    reader:close()
  end)
end)
