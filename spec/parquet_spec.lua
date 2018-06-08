describe('parquet module', function()

  it('loads', function()
    local parquet = require 'parquet'
    assert.is_true(parquet ~= nil)
  end)
  
end)
