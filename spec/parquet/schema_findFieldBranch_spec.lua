local parquet = require('parquet');

describe('ParquetSchema:findFieldBranch()', function()

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

    do
      local branch = schema:findFieldBranch('name')
      assert.not_nil(branch)
      assert.equal(1, #branch)
      local field = branch[1]
      assert.equal('name', field.name)
      assert.same({'name'}, field.path)
    end
    
    do
      local branch = schema:findFieldBranch('stock')
      assert.not_nil(branch)
      assert.equal(1, #branch)
      local field = branch[1]
      assert.equal('stock', field.name)
      assert.same({'stock'}, field.path)
    end
    
    do
      local branch = schema:findFieldBranch({'stock','quantity'})
      assert.not_nil(branch)
      assert.equal(2, #branch)
      local field = branch[1]
      assert.equal('stock', field.name)
      assert.same({'stock'}, field.path)
      field = branch[2]
      assert.same('quantity', field.name)
      assert.same({'stock','quantity'}, field.path)
    end
    
  end)

end)
