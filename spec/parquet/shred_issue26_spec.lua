local parquet = require 'parquet'
--local thrift_print_r = require 'thrift.print_r'

-- https://github.com/BixData/lua-parquet/issues/26
describe('shred issue-26', function()

  it('should materialize with deterministic results', function()
    local reader = parquet.ParquetReader.openFile('spec-fixtures/model2-data-part-00003.parquet')
    local schema = reader:getSchema()

    local buffer = {
      rowCount=1,
      columnData={}
    }
    
    buffer.columnData['id'] = {
      dlevels= {0},
      rlevels= {0},
      values= {0},
      count= 1
    }

    buffer.columnData['point,size'] = {
      dlevels= {1, 0, 0, 0, 0, 0, 0, 0},
      rlevels= {00},
      values= {},
      count= 1
    }

    buffer.columnData['point,type'] = {
      dlevels= {1, 0, 0, 0, 0, 0, 0, 0},
      rlevels= {0},
      values= {1},
      count= 1
    }

    buffer.columnData['point,indices,list,element'] = {
      dlevels= {1, 0, 0, 0, 0, 0, 0, 0},
      rlevels= {0, 0, 0, 0, 0, 0, 0, 0},
      values= {},
      count= 1
    }

    buffer.columnData['point,values,list,element'] = {
      dlevels= {3, 3, 3, 0, 0, 0, 0, 0},
      rlevels= {0, 1, 1, 0, 0, 0, 0, 0},
      values= {3, 4, 5},
      count= 3
    }

    local records = parquet.ParquetShredder.materializeRecords(schema, buffer)

    assert.equal(1, #records)

    assert.same(
      records[1],
      { id=0, point= { values= { list={{element=3}, {element=4}, {element=5}} }, type=1} })
  end)

end)
