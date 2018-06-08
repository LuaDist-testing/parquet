local parquet = require 'parquet'

local map = function(values, f)
  local r = {}
  for _, value in pairs(values) do r[#r+1] = f(value) end
  return r
end

describe('ParquetShredder', function()

  it('should shred a single simple record', function()
    local schema = parquet.ParquetSchema:new({
      name= { type='UTF8' },
      quantity= { type='INT64' },
      price= { type='DOUBLE' }
    })

    local buf = {}

    do
      local rec = { name="apple", quantity=10, price=23.5 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    local colData = buf.columnData
    assert.equal(1, buf.rowCount)
    assert.same({0}, colData.name.dlevels)
    assert.same({0}, colData.name.rlevels)
    assert.same({'apple'}, map(colData.name.values, tostring))
    assert.same({0}, colData.quantity.dlevels)
    assert.same({0}, colData.quantity.rlevels)
    assert.same({10}, colData.quantity.values)
    assert.same({0}, colData.price.dlevels)
    assert.same({0}, colData.price.rlevels)
    assert.same({23.5}, colData.price.values)
  end)

  it('should shred a list of simple records', function()
    local schema = parquet.ParquetSchema:new({
      name= { type='UTF8' },
      quantity= { type='INT64' },
      price= { type='DOUBLE' }
    })


    local buf = {}

    do
      local rec = { name="apple", quantity=10, price=23.5 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    do
      local rec = { name="orange", quantity=20, price=17.1 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    do
      local rec = { name="banana", quantity=15, price=42 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    local colData = buf.columnData
    assert.equal(3, buf.rowCount)
    assert.same({0, 0, 0}, colData.name.dlevels)
    assert.same({0, 0, 0}, colData.name.rlevels)
    assert.same({"apple", "orange", "banana"}, map(colData.name.values, tostring))
    assert.same({0, 0, 0}, colData.quantity.dlevels)
    assert.same({0, 0, 0}, colData.quantity.rlevels)
    assert.same({10, 20, 15}, colData.quantity.values)
    assert.same({0, 0, 0}, colData.price.dlevels)
    assert.same({0, 0, 0}, colData.price.rlevels)
    assert.same({23.5, 17.1, 42}, colData.price.values)
  end)

  it('should shred a list of simple records with optional scalar fields', function()
    local schema = parquet.ParquetSchema:new({
      name= { type='UTF8' },
      quantity= { type='INT64', optional=true },
      price= { type='DOUBLE' }
    })

    local buf = {}

    do
      local rec = { name="apple", quantity=10, price=23.5 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    do
      local rec = { name="orange", price=17.1 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    do
      local rec = { name="banana", quantity=15, price=42 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    local colData = buf.columnData
    assert.equal(3, buf.rowCount)
    assert.same({0, 0, 0}, colData.name.dlevels)
    assert.same({0, 0, 0}, colData.name.rlevels)
    assert.same({"apple", "orange", "banana"}, map(colData.name.values, tostring))
    assert.same({1, 0, 1}, colData.quantity.dlevels)
    assert.same({0, 0, 0}, colData.quantity.rlevels)
    assert.same({10, 15}, colData.quantity.values)
    assert.same({0, 0, 0}, colData.price.dlevels)
    assert.same({0, 0, 0}, colData.price.rlevels)
    assert.same({23.5, 17.1, 42}, colData.price.values)
  end)

  it('should shred a list of simple records with repeated scalar fields', function()
    local schema = parquet.ParquetSchema:new({
      name= { type='UTF8' },
      colours= { type='UTF8', repeated=true },
      price= { type='DOUBLE' }
    })


    local buf = {}

    do
      local rec = { name="apple", price=23.5, colours={"red", "green"} }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    do
      local rec = { name="orange", price=17.1, colours={"orange"} }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    do
      local rec = { name="banana", price=42, colours={"yellow"} }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    local colData = buf.columnData
    assert.equal(3, buf.rowCount)
    assert.same({0, 0, 0}, colData.name.dlevels)
    assert.same({0, 0, 0}, colData.name.rlevels)
    assert.same({"apple", "orange", "banana"}, map(colData.name.values, tostring))
    assert.equal(3, colData.name.count)
    assert.same({1, 1, 1, 1}, colData.colours.dlevels)
    assert.same({0, 1, 0, 0}, colData.colours.rlevels)
    assert.same({"red", "green", "orange", "yellow"}, map(colData.colours.values, tostring))
    assert.equal(4, colData.colours.count)
    assert.same({0, 0, 0}, colData.price.dlevels)
    assert.same({0, 0, 0}, colData.price.rlevels)
    assert.same({23.5, 17.1, 42}, colData.price.values)
    assert.equal(colData.price.count, 3)
  end)

  it('should shred a nested record without repetition modifiers', function()
    local schema = parquet.ParquetSchema:new({
      name= { type='UTF8' },
      stock= {
        fields= {
          quantity= { type='INT64' },
          warehouse= { type='UTF8' }
        }
      },
      price= { type='DOUBLE' }
    })


    local buf = {}

    do
      local rec = { name="apple", stock= { quantity=10, warehouse="A" }, price=23.5 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    do
      local rec = { name="banana", stock= { quantity=20, warehouse="B" }, price=42.0 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    local colData = buf.columnData
    assert.equal(2, buf.rowCount)
    assert.same({0, 0}, colData['name'].dlevels)
    assert.same({0, 0}, colData['name'].rlevels)
    assert.same({"apple", "banana"}, map(colData['name'].values, tostring))
    assert.same({0, 0}, colData['stock,quantity'].dlevels)
    assert.same({0, 0}, colData['stock,quantity'].rlevels)
    assert.same({10, 20}, colData['stock,quantity'].values)
    assert.same({0, 0}, colData['stock,warehouse'].dlevels)
    assert.same({0, 0}, colData['stock,warehouse'].rlevels)
    assert.same({"A", "B"}, map(colData['stock,warehouse'].values, tostring))
    assert.same({0, 0}, colData['price'].dlevels)
    assert.same({0, 0}, colData['price'].rlevels)
    assert.same({23.5, 42.0}, colData['price'].values)
  end)

  it('should shred a nested record with optional fields', function()
    local schema = parquet.ParquetSchema:new({
      name= { type='UTF8' },
      stock= {
        fields= {
          quantity= { type='INT64', optional=true },
          warehouse= { type='UTF8' }
        }
      },
      price= { type='DOUBLE' }
    })

    local buf = {}

    do
      local rec = { name="apple", stock= { quantity=10, warehouse="A" }, price=23.5 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    do
      local rec = { name="banana", stock= { warehouse="B" }, price=42.0 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    local colData = buf.columnData
    assert.equal(2, buf.rowCount)
    assert.same({0, 0}, colData['name'].dlevels)
    assert.same({0, 0}, colData['name'].rlevels)
    assert.same({"apple", "banana"}, map(colData['name'].values, tostring))
    assert.same({1, 0}, colData['stock,quantity'].dlevels)
    assert.same({0, 0}, colData['stock,quantity'].rlevels)
    assert.same({10}, colData['stock,quantity'].values)
    assert.same({0, 0}, colData['stock,warehouse'].dlevels)
    assert.same({0, 0}, colData['stock,warehouse'].rlevels)
    assert.same({"A", "B"}, map(colData['stock,warehouse'].values, tostring))
    assert.same({0, 0}, colData['price'].dlevels)
    assert.same({0, 0}, colData['price'].rlevels)
    assert.same({23.5, 42.0}, colData['price'].values)
  end)

  it('should shred a nested record with nested optional fields', function()
    local schema = parquet.ParquetSchema:new({
      name= { type= 'UTF8' },
      stock= {
        optional=true,
        fields= {
          quantity= { type='INT64', optional=true },
          warehouse= { type='UTF8' }
        }
      },
      price= { type='DOUBLE' },
    })

    local buf = {}

    do
      local rec = { name="apple", stock= { quantity=10, warehouse="A" }, price=23.5 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    do
      local rec = { name="orange" , price=17.0 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    do
      local rec = { name="banana", stock= { warehouse="B" }, price=42.0 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    local colData = buf.columnData
    assert.equal(3, buf.rowCount)
    assert.same({0, 0, 0}, colData['name'].dlevels)
    assert.same({0, 0, 0}, colData['name'].rlevels)
    assert.same({"apple", "orange", "banana"}, map(colData['name'].values, tostring))
    assert.same({2, 0, 1}, colData['stock,quantity'].dlevels)
    assert.same({0, 0, 0}, colData['stock,quantity'].rlevels)
    assert.same({10}, colData['stock,quantity'].values)
    assert.same({1, 0, 1}, colData['stock,warehouse'].dlevels)
    assert.same({0, 0, 0}, colData['stock,warehouse'].rlevels)
    assert.same({"A", "B"}, map(colData['stock,warehouse'].values, tostring))
    assert.same({0, 0, 0}, colData['price'].dlevels)
    assert.same({0, 0, 0}, colData['price'].rlevels)
    assert.same({23.5, 17.0, 42.0}, colData['price'].values)
  end)

  it('should shred a nested record with repeated fields', function()
    local schema = parquet.ParquetSchema:new({
      name= { type='UTF8' },
      stock= {
        fields= {
          quantity= { type='INT64', repeated=true },
          warehouse= { type='UTF8' }
        }
      },
      price= { type='DOUBLE' }
    })

    local buf = {}

    do
      local rec = { name="apple", stock= { quantity=10, warehouse="A" }, price=23.5 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    do
      local rec = { name="orange", stock= { quantity={50, 75}, warehouse="B" }, price=17.0 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    do
      local rec = { name="banana", stock= { warehouse="C" }, price=42.0 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    local colData = buf.columnData
    assert.equal(3, buf.rowCount)
    assert.same({0, 0, 0}, colData['name'].dlevels)
    assert.same({0, 0, 0}, colData['name'].rlevels)
    assert.same({"apple", "orange", "banana"}, map(colData['name'].values, tostring))
    assert.same({1, 1, 1, 0}, colData['stock,quantity'].dlevels)
    assert.same({0, 0, 1, 0}, colData['stock,quantity'].rlevels)
    assert.same({10, 50, 75}, colData['stock,quantity'].values)
    assert.same({0, 0, 0}, colData['stock,warehouse'].dlevels)
    assert.same({0, 0, 0}, colData['stock,warehouse'].rlevels)
    assert.same({"A", "B", "C"}, map(colData['stock,warehouse'].values, tostring))
    assert.same({0, 0, 0}, colData['price'].dlevels)
    assert.same({0, 0, 0}, colData['price'].rlevels)
    assert.same({23.5, 17.0, 42.0}, colData['price'].values)
  end)

  it('should shred a nested record with nested repeated fields', function()
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


    local buf = {}

    do
      local rec = { name="apple", stock= {{ quantity=10, warehouse="A" }, { quantity=20, warehouse="B" } }, price=23.5 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    do
      local rec = { name="orange", stock= { quantity={50, 75}, warehouse="X" }, price=17.0 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    do
      local rec = { name="kiwi", price=99.0 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    do
      local rec = { name="banana", stock= { warehouse="C" }, price=42.0 }
      parquet.ParquetShredder.shredRecord(schema, rec, buf)
    end

    local colData = buf.columnData
    assert.equal(4, buf.rowCount)
    assert.same({0, 0, 0, 0}, colData['name'].dlevels)
    assert.same({0, 0, 0, 0}, colData['name'].rlevels)
    assert.same({"apple", "orange", "kiwi", "banana"}, map(colData['name'].values, tostring))
    assert.same({2, 2, 2, 2, 0, 1}, colData['stock,quantity'].dlevels)
    assert.same({0, 1, 0, 2, 0, 0}, colData['stock,quantity'].rlevels)
    assert.same({10, 20, 50, 75}, colData['stock,quantity'].values)
    assert.same({1, 1, 1, 0, 1}, colData['stock,warehouse'].dlevels)
    assert.same({0, 1, 0, 0, 0}, colData['stock,warehouse'].rlevels)
    assert.same({"A", "B", "X", "C"}, map(colData['stock,warehouse'].values, tostring))
    assert.same({0, 0, 0, 0}, colData['price'].dlevels)
    assert.same({0, 0, 0, 0}, colData['price'].rlevels)
    assert.same({23.5, 17.0, 99.0, 42.0}, colData['price'].values)
  end)

  it('should materialize a nested record with scalar repeated fields', function()
    local schema = parquet.ParquetSchema:new({
      name= { type='UTF8' },
      price= { type='DOUBLE', repeated=true }
    })

    local buffer = {
      rowCount=4,
      columnData={}
    }

    buffer.columnData['name'] = {
      dlevels={0, 0, 0, 0},
      rlevels={0, 0, 0, 0},
      values={
        '\97\112\112\108\101',
        '\111\114\97\110\103\101',
        '\107\105\119\105',
        '\98\97\110\97\110\97'
      },
      count=4
    }

    buffer.columnData['price'] = {
      dlevels={1, 1, 1, 1, 1, 1},
      rlevels={0, 0, 1, 0, 1, 0},
      values={23.5, 17, 23, 99, 100, 42},
      count=6
    }

    local records = parquet.ParquetShredder.materializeRecords(schema, buffer)

    assert.equal(4, #records)

    assert.same({ name="apple", price={23.5} }   , records[1])
    assert.same({ name="orange", price={17, 23} }, records[2])
    assert.same({ name="kiwi", price={99, 100} } , records[3])
    assert.same({ name="banana", price={42} }    , records[4])
  end)

  it('should materialize a nested record with nested repeated fields', function()
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

    local buffer = {
      rowCount=4,
      columnData={}
    }

    buffer.columnData['name'] = {
      dlevels= {0, 0, 0, 0},
      rlevels= {0, 0, 0, 0},
      values={
        '\97\112\112\108\101',
        '\111\114\97\110\103\101',
        '\107\105\119\105',
        '\98\97\110\97\110\97'
      },
      count=4
    }

    buffer.columnData['stock,quantity'] = {
      dlevels= {2, 2, 2, 2, 0, 1},
      rlevels= {0, 1, 0, 2, 0, 0},
      values= {10, 20, 50, 75},
      count= 6
    }

    buffer.columnData['stock,warehouse'] = {
      dlevels= {1, 1, 1, 0, 1},
      rlevels= {0, 1, 0, 0, 0},
      values= {
        '\65',
        '\66',
        '\88',
        '\67'
      },
      count=5
    }

    buffer.columnData['price'] = {
      dlevels= {0, 0, 0, 0},
      rlevels= {0, 0, 0, 0},
      values= {23.5, 17, 99, 42},
      count= 4
    }

    local records = parquet.ParquetShredder.materializeRecords(schema, buffer)

    assert.equal(4, #records)

    assert.same(
      { name="apple", stock={{ quantity={10}, warehouse="A" }, { quantity={20}, warehouse="B" } }, price=23.5 },
      records[1])

    assert.same(
        records[2],
        { name="orange", stock= {{ quantity={50, 75}, warehouse="X" }}, price=17.0 })

    assert.same(
        records[3],
        { name="kiwi", price=99.0 })

    assert.same(
        records[4],
        { name="banana", stock= {{ warehouse="C" }}, price=42.0 })
  end)

end)
