local compression = require 'parquet.compression'

describe('compression', function()

  it('UNCOMPRESSED deflate works', function()
    assert.equal('abc', compression.deflate('UNCOMPRESSED', 'abc'))
  end)
  
  it('UNCOMPRESSED inflate works', function()
    assert.equal('def', compression.inflate('UNCOMPRESSED', 'def'))
  end)
  
  it('asserts when requesting a non-existent compression', function()
    assert.has_error(function() compression.deflate('foo', 0) end)
  end)
  
  insulate('registration of a new compression', function()
    it('deflate works', function()
      local deflateFn = spy.new(function() end)
      local inflateFn = spy.new(function() end)
      compression.register('ATARI', deflateFn, inflateFn)
      compression.deflate('ATARI', 'abc')
      assert.spy(deflateFn).was_called_with('abc')
      assert.spy(inflateFn).was_not_called()
    end)
    it('inflate works', function()
      local deflateFn = spy.new(function() end)
      local inflateFn = spy.new(function() end)
      compression.register('ATARI', deflateFn, inflateFn)
      compression.inflate('ATARI', 'def')
      assert.spy(inflateFn).was_called_with('def')
      assert.spy(deflateFn).was_not_called()
    end)
  end)
  
end)
