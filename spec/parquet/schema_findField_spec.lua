local parquet = require('parquet');

describe('ParquetSchema:findField()', function()

  it('should return correct fields from a nested schema', function()
    local schema = parquet.ParquetSchema:new({
      name= { type='UTF8' },
      stock= {
        repeated=true,
        fields= {
          quantity= { type='INT64', repeated=true },
          warehouse= { type='UTF8' }
        }
      },
      price= { type='DOUBLE' }
    })

    local field = schema:findField('name')
    assert.not_nil(field)
    assert.equal('name', field.name)
    assert.same({'name'}, field.path)

    field = schema:findField('stock')
    assert.not_nil(field)
    assert.equal('stock', field.name)
    assert.same({'stock'}, field.path)
    
    field = schema:findField({'stock','quantity'})
    assert.not_nil(field)
    assert.same('quantity', field.name)
    assert.same({'stock','quantity'}, field.path)
    
    field = schema:findField('stock,quantity')
    assert.not_nil(field)
    assert.same('quantity', field.name)
    assert.same({'stock','quantity'}, field.path)
    
  end)

end)
