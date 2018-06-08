local parquet_ttypes = require 'parquet.parquet_ttypes'
local parquet_util = require 'parquet.util'

describe('Thrift', function()

  it('should correctly en/decode literal zeroes with the CompactProtocol', function()
    local obj = parquet_ttypes.ColumnMetaData:new()
--    print('ColumnMetadata', obj)
    obj.num_values = 0

    parquet_util.serializeThrift(obj)
    --assert.equal(3, #obj_bin)
  end)

end)
