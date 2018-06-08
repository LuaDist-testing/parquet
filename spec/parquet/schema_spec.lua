local parquet = require('parquet')

local tableLength = function(t)
  local c = 0
  for _ in pairs(t) do c = c + 1 end
  return c
end

describe('ParquetSchema', function()

  it('should assign correct defaults in a simple flat schema', function()
    local schema = parquet.ParquetSchema:new({
      name={ type='UTF8' },
      quantity= { type='INT64' },
      price= { type='DOUBLE' }
    })

    assert.equal(3, tableLength(schema.fieldList))
    assert.is_not_nil(schema.fields.name)
    assert.is_not_nil(schema.fields.quantity)
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
      local c = schema.fields.quantity
      assert.equal('quantity', c.name)
      assert.equal('INT64', c.primitiveType)
      assert.equal(nil, c.originalType)
      assert.same({'quantity'}, c.path)
      assert.equal('REQUIRED', c.repetitionType)
      assert.equal('PLAIN', c.encoding)
      assert.equal('UNCOMPRESSED', c.compression)
      assert.equal(0, c.rLevelMax)
      assert.equal(0, c.dLevelMax)
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

  end)

  it('should assign correct defaults in a flat schema with optional fieldList', function()
    local schema = parquet.ParquetSchema:new({
      name= { type='UTF8' },
      quantity= { type='INT64', optional=true },
      price= { type='DOUBLE' }
    })

    assert.equal(3, tableLength(schema.fieldList), 3)
    assert.is_not_nil(schema.fields.name)
    assert.is_not_nil(schema.fields.quantity)
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
      local c = schema.fields.quantity
      assert.equal('quantity', c.name)
      assert.equal('INT64', c.primitiveType)
      assert.equal(nil, c.originalType)
      assert.same({'quantity'}, c.path)
      assert.equal('OPTIONAL', c.repetitionType)
      assert.equal('PLAIN', c.encoding)
      assert.equal('UNCOMPRESSED', c.compression)
      assert.equal(0, c.rLevelMax)
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
  end)

  it('should assign correct defaults in a flat schema with repeated fieldList', function()
    local schema = parquet.ParquetSchema:new({
      name= { type='UTF8' },
      quantity= { type='INT64', repeated=true },
      price= { type='DOUBLE' },
    })

    assert.equal(3, tableLength(schema.fieldList))
    assert.is_not_nil(schema.fields.name)
    assert.is_not_nil(schema.fields.quantity)
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
      local c = schema.fields.quantity
      assert.equal('quantity', c.name)
      assert.equal('INT64', c.primitiveType)
      assert.equal(nil, c.originalType)
      assert.same({'quantity'}, c.path)
      assert.equal('REPEATED', c.repetitionType)
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
  end)

  it('should assign correct defaults in a nested schema without repetition modifiers', function()
    local schema = parquet.ParquetSchema:new({
      name= { type='UTF8' },
      stock= {
        fields= {
          quantity= { type='INT64' },
          warehouse= { type='UTF8' },
        }
      },
      price= { type='DOUBLE' },
    })

    assert.equal(5, tableLength(schema.fieldList))
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
      assert.equal('REQUIRED', c.repetitionType)
      assert.equal(nil, c.encoding)
      assert.equal(nil, c.compression)
      assert.equal(0, c.rLevelMax)
      assert.equal(0, c.dLevelMax)
      assert.equal(true, not not c.isNested)
      assert.equal(2, c.fieldCount)
    end

    do
      local c = schema.fields.stock.fields.quantity
      assert.equal('quantity', c.name)
      assert.equal('INT64', c.primitiveType)
      assert.equal(nil, c.originalType)
      assert.same({'stock', 'quantity'}, c.path)
      assert.equal('REQUIRED', c.repetitionType)
      assert.equal('PLAIN', c.encoding)
      assert.equal('UNCOMPRESSED', c.compression)
      assert.equal(0, c.rLevelMax)
      assert.equal(0, c.dLevelMax)
      assert.equal(false, not not c.isNested)
      assert.equal(nil, c.fieldCount)
    end

    do
      local c = schema.fields.stock.fields.warehouse
      assert.equal('warehouse', c.name)
      assert.equal('BYTE_ARRAY', c.primitiveType)
      assert.equal('UTF8', c.originalType)
      assert.same({'stock', 'warehouse'}, c.path)
      assert.equal('REQUIRED', c.repetitionType)
      assert.equal('PLAIN', c.encoding)
      assert.equal('UNCOMPRESSED', c.compression)
      assert.equal(0, c.rLevelMax)
      assert.equal(0, c.dLevelMax)
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
  end)

  it('should assign correct defaults in a nested schema with optional fields', function()
    local schema = parquet.ParquetSchema:new({
      name= { type='UTF8' },
      stock= {
        optional=true,
        fields= {
          quantity= { type='INT64', optional=true },
          warehouse= { type='UTF8' }
        }
      },
      price= { type='DOUBLE' }
    })

    assert.equal(5, tableLength(schema.fieldList));
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
      assert.equal('OPTIONAL', c.repetitionType)
      assert.equal(nil, c.encoding)
      assert.equal(nil, c.compression)
      assert.equal(0, c.rLevelMax)
      assert.equal(1, c.dLevelMax)
      assert.equal(true, not not c.isNested)
      assert.equal(2, c.fieldCount)
    end

    do
      local c = schema.fields.stock.fields.quantity
      assert.equal('quantity', c.name)
      assert.equal('INT64', c.primitiveType)
      assert.equal(nil, c.originalType)
      assert.same({'stock', 'quantity'}, c.path)
      assert.equal('OPTIONAL', c.repetitionType)
      assert.equal('PLAIN', c.encoding)
      assert.equal('UNCOMPRESSED', c.compression)
      assert.equal(0, c.rLevelMax)
      assert.equal(2, c.dLevelMax)
      assert.equal(false, not not c.isNested)
      assert.equal(nil, c.fieldCount)
    end

    do
      local c = schema.fields.stock.fields.warehouse
      assert.equal('warehouse', c.name)
      assert.equal('BYTE_ARRAY', c.primitiveType)
      assert.equal('UTF8', c.originalType)
      assert.same({'stock', 'warehouse'}, c.path)
      assert.equal('REQUIRED', c.repetitionType)
      assert.equal('PLAIN', c.encoding)
      assert.equal('UNCOMPRESSED', c.compression)
      assert.equal(0, c.rLevelMax)
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
  end)

  it('should assign correct defaults in a nested schema with repeated fields', function()
    local schema = parquet.ParquetSchema:new({
      name= { type='UTF8' },
      stock= {
        repeated=true,
        fields= {
          quantity= { type='INT64', optional=true },
          warehouse= { type='UTF8' }
        }
      },
      price= { type='DOUBLE' }
    })

    assert.equal(5, tableLength(schema.fieldList))
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
      assert.same({'stock', 'quantity'}, c.path)
      assert.equal('OPTIONAL', c.repetitionType)
      assert.equal('PLAIN', c.encoding)
      assert.equal('UNCOMPRESSED', c.compression)
      assert.equal(1, c.rLevelMax)
      assert.equal(2, c.dLevelMax)
      assert.equal(false, not not c.isNested)
      assert.equal(nil, c.fieldCount)
    end

    do
      local c = schema.fields.stock.fields.warehouse
      assert.equal('warehouse', c.name)
      assert.equal('BYTE_ARRAY', c.primitiveType)
      assert.equal('UTF8', c.originalType)
      assert.same({'stock', 'warehouse'}, c.path)
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
  end)

end)
